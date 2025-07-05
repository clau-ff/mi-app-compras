import streamlit as st
import pandas as pd
import sqlite3
from fuzzywuzzy import fuzz
from PIL import Image, ExifTags
import tempfile
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import gspread
import os
import base64

# LOGIN SIMPLE
st.title("App Compras Familiares")
PASSWORD = st.secrets["CLAVE_FAMILIAR"]
password_input = st.text_input("Ingresa la clave familiar", type="password")
if password_input != PASSWORD:
    st.warning("Clave incorrecta o pendiente de ingresar.")
    st.stop()
st.success("¡Bienvenida/o!")

# ----- GOOGLE DRIVE SERVICE ACCOUNT -----
if not "SERVICE_ACCOUNT_JSON" in st.secrets:
    st.error("No se encontró SERVICE_ACCOUNT_JSON en tus Secrets.")
    st.stop()
SERVICE_ACCOUNT_FILE = "service_account.json"
if not st.session_state.get("service_account_file_created", False):
    with open(SERVICE_ACCOUNT_FILE, "w") as f:
        f.write(st.secrets["SERVICE_ACCOUNT_JSON"])
    st.session_state["service_account_file_created"] = True

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]
credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES
)
drive_service = build("drive", "v3", credentials=credentials)

# Función para buscar ID de carpeta por nombre
def buscar_carpeta(nombre, parent_id=None):
    q = f"name = '{nombre}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        q += f" and '{parent_id}' in parents"
    results = drive_service.files().list(q=q, fields="files(id, name)").execute()
    files = results.get('files', [])
    if not files:
        return None
    return files[0]['id']

# Buscar IDs de carpetas
bluecoins_id = buscar_carpeta('Bluecoins')
quicksync_id = buscar_carpeta('QuickSync', parent_id=bluecoins_id)
pictures_id = buscar_carpeta('Pictures', parent_id=bluecoins_id)

# Buscar archivo Bluecoins más reciente
q = f"'{quicksync_id}' in parents and trashed = false and name contains '.fydb'"
results = drive_service.files().list(q=q, fields="files(id, name, modifiedTime)").execute()
fydb_files = results.get('files', [])
if not fydb_files:
    st.error("No se encontraron archivos .fydb en la carpeta QuickSync.")
    st.stop()
fydb_files.sort(key=lambda x: x['modifiedTime'], reverse=True)
latest_file = fydb_files[0]
request = drive_service.files().get_media(fileId=latest_file['id'])
fh = open("bluecoins.fydb", "wb")
from googleapiclient.http import MediaIoBaseDownload
downloader = MediaIoBaseDownload(fh, request)
done = False
while done is False:
    status, done = downloader.next_chunk()
fh.close()
# Mostrar fecha y hora de la última versión de la base bluecoins encontrada
fecha_modif = latest_file['modifiedTime']
st.info(f"Archivo descargado: {latest_file['name']} (última modificación: {fecha_modif.replace('T', ' ').replace('Z','')})")

# Leer tablas de SQLite
DB_FILENAME = "bluecoins.fydb"
@st.cache_data
def leer_tabla(nombre_tabla):
    with sqlite3.connect(DB_FILENAME) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {nombre_tabla}", conn)
    return df

df_trans = leer_tabla("TRANSACTIONSTABLE")
from datetime import datetime

# Convierte a datetime el campo 'date'
df_trans['date'] = pd.to_datetime(df_trans['date'], errors='coerce')
# Filtra solo hasta hoy
hoy = pd.Timestamp(datetime.now().date())
df_trans = df_trans[df_trans['date'] <= hoy]

df_item = leer_tabla("ITEMTABLE")
df_pic = leer_tabla("PICTURETABLE")

# ---- Búsqueda flexible de producto ----
nombre_producto = st.text_input("Escribe el nombre del producto que quieres buscar:", key="nombre_producto")

if nombre_producto:
    # Solo transacciones CON boleta
    trans_ids_con_boleta = set(df_pic['transactionID'].unique())
    df_trans_boleta = df_trans[df_trans['transactionsTableID'].isin(trans_ids_con_boleta)].copy()
    
    # Coincidencias exactas primero
    exactos = df_trans_boleta[df_trans_boleta['notes'].str.contains(nombre_producto, case=False, na=False)]
    exactos = exactos.sort_values("date", ascending=False)
    top = exactos.head(3)
    
    # Si menos de 3, completa con fuzzy entre el resto
    if len(top) < 3:
        ids_ya = set(top['transactionsTableID'])
        resto = df_trans_boleta[~df_trans_boleta['transactionsTableID'].isin(ids_ya)].copy()
        resto['fuzzy_score'] = resto['notes'].astype(str).apply(lambda x: fuzz.partial_ratio(nombre_producto.lower(), x.lower()))
        resto = resto[resto['fuzzy_score'] > 70]
        resto = resto.sort_values("fuzzy_score", ascending=False)
        n_faltan = 3 - len(top)
        top = pd.concat([top, resto.head(n_faltan)])
    
    SHEETS_ID = st.secrets["SHEETS_ID"]
    gc = gspread.authorize(credentials)
    worksheet = gc.open_by_key(SHEETS_ID).sheet1  # usa la hoja 1
    filas = worksheet.get_all_records()
    ids_existentes = worksheet.col_values(2)
    top = top.sort_values("date", ascending=False)
    
    def descargar_y_mostrar_imagen(drive_service, file_name, carpeta_id):
        q = f"'{carpeta_id}' in parents and trashed = false and name = '{file_name}'"
        results = drive_service.files().list(q=q, fields="files(id, name)").execute()
        files = results.get('files', [])
        if not files:
            return None
        file_id = files[0]['id']
        request = drive_service.files().get_media(fileId=file_id)
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix="." + file_name.split(".")[-1])
        fh = open(tmp_file.name, "wb")
        from googleapiclient.http import MediaIoBaseDownload
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.close()
        return tmp_file.name

    for idx, row in top.iterrows():
        comercio = None
        archivo_img = None
        ruta_local = None
        if 'itemID' in row and not pd.isnull(row['itemID']):
            item_row = df_item[df_item['itemTableID'] == row['itemID']]
            if not item_row.empty:
                comercio = item_row.iloc[0]['itemName']

        # BUSCAR imagen AHORA
        pic = df_pic[df_pic['transactionID'] == row['transactionsTableID']]
        if not pic.empty:
            archivo_img = pic.iloc[0]['pictureFileName']
            ruta_local = descargar_y_mostrar_imagen(drive_service, archivo_img, pictures_id)

        st.markdown(f"- **Fecha:** {row['date'].strftime('%Y-%m-%d') if not pd.isnull(row['date']) else row['date']}")
        st.markdown(f"  - **Nota:** {row['notes']}")
        if comercio:
            st.markdown(f"  - **Lugar/Comercio:** {comercio}")
        if archivo_img:
            st.markdown(f"  - **Nombre archivo imagen:** {archivo_img}")

        mostrar_formulario = False

        if ruta_local:
            ext = os.path.splitext(ruta_local)[-1].lower()
            if ext in [".jpg", ".jpeg", ".png"]:
                imagen = Image.open(ruta_local)
                width, height = imagen.size
                if width > height:
                    imagen = imagen.rotate(90, expand=True)
                max_width = 700
                if imagen.size[0] > max_width:
                    new_height = int(max_width * imagen.size[1] / imagen.size[0])
                    imagen = imagen.resize((max_width, new_height))
                st.image(imagen, caption="Boleta", use_container_width=True)
                mostrar_formulario = True
            elif ext == ".pdf":
                with open(ruta_local, "rb") as f:
                    base64_pdf = base64.b64encode(f.read()).decode('utf-8')
                pdf_display = f"""
                    <iframe src="data:application/pdf;base64,{base64_pdf}" width="700" height="900" type="application/pdf"></iframe>
                """
                st.components.v1.html(pdf_display, height=920)
                with open(ruta_local, "rb") as f:
                    st.download_button(
                        label="Descargar boleta PDF",
                        data=f,
                        file_name=os.path.basename(ruta_local),
                        mime="application/pdf"
                    )
                st.info("Si no ves el PDF arriba, tu navegador lo bloqueó. Haz clic en 'Descargar boleta PDF'.")
                mostrar_formulario = True
            else:
                st.warning("Formato de archivo no soportado para previsualización.")
                mostrar_formulario = False
        else:
            st.info("No hay archivo de boleta disponible.")

        if mostrar_formulario:
            with st.form(f"form_{row['transactionsTableID']}"):
                precio = st.number_input("Precio pagado", min_value=0.0, format="%.2f", key=f"precio_{row['transactionsTableID']}")
                cantidad = st.number_input("Cantidad comprada", min_value=1, step=1, key=f"cantidad_{row['transactionsTableID']}")
                submit = st.form_submit_button("Registrar en Google Sheets")
            if submit:
                if str(row['transactionsTableID']) not in ids_existentes:
                    worksheet.append_row([
                        nombre_producto,  # Producto buscado
                        str(row['transactionsTableID']),
                        row['date'].strftime('%Y-%m-%d') if not pd.isnull(row['date']) else row['date'],
                        row['notes'],
                        comercio if comercio else '',
                        archivo_img if archivo_img else '',
                        precio,
                        cantidad
                    ])
                    st.success("¡Compra guardada en Google Sheets!")
                    ids_existentes.append(str(row['transactionsTableID']))
                else:
                    st.info("Ya existe un registro para esta transacción en Sheets, no se duplicó.")

    st.markdown("---")

    # --- ANÁLISIS Y RECOMENDACIÓN SÓLO SI TODAS LAS COMPRAS ESTÁN REGISTRADAS ---
    trans_ids_mostradas = set(str(row['transactionsTableID']) for idx, row in top.iterrows())
    registros_en_sheet = set(ids_existentes)

    if trans_ids_mostradas.issubset(registros_en_sheet):
        precios_cantidades = [
            (float(f['Precio']), float(f['Cantidad']), pd.to_datetime(f['Fecha'], errors='coerce'))
            for f in filas
            if 'Producto buscado' in f and f['Producto buscado'].strip().lower() == nombre_producto.strip().lower()
            and f['Precio'] not in ('', None) and f['Cantidad'] not in ('', None)
        ]
        if precios_cantidades:
            suma_precios_x_cant = sum(p * c for p, c, _ in precios_cantidades)
            suma_cantidades = sum(c for _, c, _ in precios_cantidades)
            precio_min = min(p for p, _, _ in precios_cantidades)
            precio_prom_pond = suma_precios_x_cant / suma_cantidades if suma_cantidades > 0 else 0

            st.subheader("Análisis de precios históricos (ponderado por cantidad):")
            st.write(f"- Precio mínimo: {precio_min:.2f}")
            st.write(f"- Precio promedio ponderado: {precio_prom_pond:.2f}")

            # Consultar PRECIO VIGENTE
            precio_vigente = st.number_input("¿Cuál es el precio vigente del producto?", min_value=0.0, format="%.2f")
            if precio_vigente > 0:
                if precio_vigente <= precio_prom_pond:
                    fechas_ordenadas = sorted([f for _, _, f in precios_cantidades if not pd.isnull(f)])
                    if len(fechas_ordenadas) > 1:
                        meses = max(1, ((fechas_ordenadas[-1] - fechas_ordenadas[0]).days // 30))
                    else:
                        meses = 1
                    consumo_prom_mensual = suma_cantidades / meses if meses else suma_cantidades
                    cantidad_recomendada = int(round(consumo_prom_mensual * 3))

                    fecha_ultima = max(fechas_ordenadas)
                    dias_desde_ultima = (pd.Timestamp(datetime.now().date()) - fecha_ultima).days

                    st.success(
                        f"¡Conviene comprar! Cantidad recomendada para 3 meses: **{cantidad_recomendada}** unidades.\n"
                        f"(Consumo promedio mensual: {consumo_prom_mensual:.2f}; Última compra: {fecha_ultima.strftime('%Y-%m-%d')}, hace {dias_desde_ultima} días)"
                    )
                else:
                    st.info("El precio vigente NO conviene (es mayor al precio promedio). No se recomienda comprar.")
        else:
            st.info("No hay historial suficiente para analizar precios aún.")
    else:
        st.info("Debes ingresar precio y cantidad para todas las compras mostradas antes de analizar precios.")
else:
    st.info("Por favor, ingresa el nombre del producto a buscar.")
