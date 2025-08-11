# Versión 3 de la App Compras Familiares (app7.py)
# Mejora: reposición automática al descartar, muestra Nota antes de boleta, PDF con iframe+descarga+aviso

import streamlit as st
import pandas as pd
import sqlite3
from fuzzywuzzy import fuzz
from PIL import Image
import tempfile
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import gspread
import os
import base64
from datetime import datetime
from googleapiclient.http import MediaIoBaseDownload
import unicodedata

st.title("App Compras Familiares v3")
PASSWORD = st.secrets["CLAVE_FAMILIAR"]
password_input = st.text_input("Ingresa la clave familiar", type="password")
if password_input != PASSWORD:
    st.warning("Clave incorrecta o pendiente de ingresar.")
    st.stop()
st.success("¡Bienvenida/o!")

# --- Google Drive / Sheets ---
if "SERVICE_ACCOUNT_JSON" not in st.secrets:
    st.error("No se encontró SERVICE_ACCOUNT_JSON en tus Secrets.")
    st.stop()
SERVICE_ACCOUNT_FILE = "service_account.json"
if not st.session_state.get("service_account_file_created", False):
    with open(SERVICE_ACCOUNT_FILE, "w") as f:
        f.write(st.secrets["SERVICE_ACCOUNT_JSON"])
    st.session_state["service_account_file_created"] = True

SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build("drive", "v3", credentials=credentials)
gc = gspread.authorize(credentials)
worksheet = gc.open_by_key(st.secrets["SHEETS_ID"]).sheet1
filas = worksheet.get_all_records()
ids_existentes = worksheet.col_values(2)

# --- Utilidades Drive ---
def buscar_carpeta(nombre, parent_id=None):
    q = f"name = '{nombre}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        q += f" and '{parent_id}' in parents"
    results = drive_service.files().list(q=q, fields="files(id, name)").execute()
    files = results.get('files', [])
    if not files:
        return None
    return files[0]['id']

bluecoins_id = buscar_carpeta('Bluecoins')
quicksync_id = buscar_carpeta('QuickSync', parent_id=bluecoins_id)
pictures_id = buscar_carpeta('Pictures', parent_id=bluecoins_id)

# --- Descargar .fydb más reciente ---
q = f"'{quicksync_id}' in parents and trashed = false and name contains '.fydb'"
results = drive_service.files().list(q=q, fields="files(id, name, modifiedTime)").execute()
fydb_files = results.get('files', [])
if not fydb_files:
    st.error("No se encontraron archivos .fydb en la carpeta QuickSync.")
    st.stop()
fydb_files.sort(key=lambda x: x['modifiedTime'], reverse=True)
latest_file = fydb_files[0]
request = drive_service.files().get_media(fileId=latest_file['id'])
with open("bluecoins.fydb", "wb") as fh:
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
fecha_modif = latest_file['modifiedTime']
st.info(f"Archivo descargado: {latest_file['name']} (última modificación: {fecha_modif.replace('T',' ').replace('Z','')})")

# --- Lectura de tablas ---
@st.cache_data
def leer_tabla(nombre_tabla):
    with sqlite3.connect("bluecoins.fydb") as conn:
        return pd.read_sql_query(f"SELECT * FROM {nombre_tabla}", conn)

df_trans = leer_tabla("TRANSACTIONSTABLE")
df_trans['date'] = pd.to_datetime(df_trans['date'], errors='coerce')
df_trans = df_trans[df_trans['date'] <= pd.Timestamp(datetime.now().date())]
df_item = leer_tabla("ITEMTABLE")
df_pic = leer_tabla("PICTURETABLE")

# --- Normalización texto ---
def normalizar(texto):
    if not isinstance(texto, str):
        return ""
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
    return texto.lower().strip()

# --- UI búsqueda ---
nombre_producto = st.text_input("Producto a buscar:")
solo_con_boleta = st.radio("¿Mostrar sólo registros con boleta?", options=["Sí", "No"], index=0)
n_resultados = st.number_input("¿Cuántas compras quieres ver?", min_value=3, max_value=20, value=3)

if nombre_producto:
    # Estado de descartados
    if "descartados" not in st.session_state:
        st.session_state.descartados = set()

    # Prepara dataset base según filtro boleta
    nombre_normalizado = normalizar(nombre_producto)
    df_trans = df_trans.copy()
    df_trans['notes_norm'] = df_trans['notes'].apply(normalizar)
    if solo_con_boleta == "Sí":
        trans_ids_con_boleta = set(df_pic['transactionID'].unique())
        df_base = df_trans[df_trans['transactionsTableID'].isin(trans_ids_con_boleta)].copy()
    else:
        df_base = df_trans.copy()

    # Función que construye TOP considerando descartes + fuzzy para reponer
    def construir_top():
        exactos = df_base[df_base['notes_norm'].str.contains(nombre_normalizado, na=False)].copy()
        exactos = exactos.sort_values("date", ascending=False)
        # quitar descartados
        exactos = exactos[~exactos['transactionsTableID'].isin(st.session_state.descartados)]
        top = exactos.copy()

        if len(top) < n_resultados:
            ya_tomados = set(top['transactionsTableID']) | st.session_state.descartados
            resto = df_base[~df_base['transactionsTableID'].isin(ya_tomados)].copy()
            resto['score'] = resto['notes_norm'].apply(lambda x: fuzz.partial_ratio(nombre_normalizado, x))
            resto = resto[resto['score'] > 70].sort_values("score", ascending=False)
            top = pd.concat([top, resto.head(int(n_resultados - len(top)))], ignore_index=True)

        # Orden final por fecha y limitar a N
        top = top.drop_duplicates('transactionsTableID').sort_values("date", ascending=False).head(int(n_resultados))
        return top

    top = construir_top()

    if top.empty:
        st.warning("No se encontraron registros suficientes con los criterios seleccionados.")

    trans_ids_mostradas = set()

    # Descarga de imagen/PDF
    def descargar_archivo_boleta(file_name):
        q = f"'{pictures_id}' in parents and trashed = false and name = '{file_name}'"
        results = drive_service.files().list(q=q, fields="files(id, name)").execute()
        files = results.get('files', [])
        if not files:
            return None
        file_id = files[0]['id']
        request = drive_service.files().get_media(fileId=file_id)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix="." + file_name.split(".")[-1])
        with open(tmp.name, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        return tmp.name

    # Render de cada registro
    for _, row in top.iterrows():
        tid = row['transactionsTableID']
        if tid in st.session_state.descartados:
            continue
        trans_ids_mostradas.add(str(tid))

        # Botón descartar: marca, reconstruye en el siguiente rerun y repone hasta completar N
        col1, col2 = st.columns([1, 3])
        with col1:
            if st.button("Descartar", key=f"descartar_{tid}"):
                st.session_state.descartados.add(tid)
                st.rerun()

        with col2:
            # Mostrar Nota ANTES de boleta (restaurado)
            st.markdown(f"**Nota:** {row['notes']}")

            # Mostrar boleta si existe
            archivo_img = df_pic[df_pic['transactionID'] == tid]['pictureFileName'].values
            if len(archivo_img) > 0:
                ruta = descargar_archivo_boleta(archivo_img[0])
                if ruta:
                    ext = os.path.splitext(ruta)[-1].lower()
                    if ext in [".jpg", ".jpeg", ".png"]:
                        imagen = Image.open(ruta)
                        if imagen.width > imagen.height:
                            imagen = imagen.rotate(90, expand=True)
                        st.image(imagen, caption="Boleta")
                    elif ext == ".pdf":
                        # PDF igual que v2: iframe + botón descarga + aviso
                        with open(ruta, "rb") as f:
                            base64_pdf = base64.b64encode(f.read()).decode('utf-8')
                        pdf_display = f"""
                            <iframe src="data:application/pdf;base64,{base64_pdf}" width="700" height="900" type="application/pdf"></iframe>
                        """
                        st.components.v1.html(pdf_display, height=920)
                        with open(ruta, "rb") as f:
                            st.download_button(
                                label="Descargar boleta PDF",
                                data=f,
                                file_name=os.path.basename(ruta),
                                mime="application/pdf"
                            )
                        st.info("Si no ves el PDF arriba, tu navegador lo bloqueó. Haz clic en 'Descargar boleta PDF'.")

        # Form para guardar en Sheets
        with st.form(f"form_{tid}"):
            precio = st.number_input("Precio", min_value=0, key=f"precio_{tid}")
            cantidad = st.number_input("Cantidad", min_value=1, step=1, key=f"cantidad_{tid}")
            unidad = st.text_input("Unidad (ej: kg, L)", key=f"unidad_{tid}")
            submit = st.form_submit_button("Guardar")
            if submit:
                if str(tid) not in ids_existentes:
                    comercio = ''
                    if not pd.isnull(row.get('itemID')):
                        item_row = df_item[df_item['itemTableID'] == row['itemID']]
                        if not item_row.empty:
                            comercio = item_row.iloc[0]['itemName']
                    worksheet.append_row([
                        nombre_producto, str(tid), row['date'].strftime('%Y-%m-%d'), row['notes'],
                        comercio, archivo_img[0] if len(archivo_img) > 0 else '', precio, cantidad, unidad
                    ])
                    st.success("¡Compra registrada!")
                    st.rerun()

    # --- Análisis extendido (sin cambios de lógica aquí) ---
    registros_sheet = set(worksheet.col_values(2))
    if trans_ids_mostradas and trans_ids_mostradas.issubset(registros_sheet):
        datos = [
            (float(f['Precio']), float(f['Cantidad']), f.get('Unidad', ''), pd.to_datetime(f['Fecha'], errors='coerce'))
            for f in worksheet.get_all_records()
            if normalizar(f['Producto buscado']) == nombre_normalizado
            and f.get('Unidad', '') != '' and f['Precio'] not in ('', None, '') and f['Cantidad'] not in ('', None, '')
        ]
        if datos:
            st.subheader("📊 Análisis de compras por unidad")
            unidades = set(u for _, _, u, _ in datos if u)
            for unidad in unidades:
                subset = [(p, c, d) for p, c, u2, d in datos if u2 == unidad and p > 0 and c > 0 and not pd.isnull(d)]
                if not subset:
                    continue
                precios, cantidades, fechas = zip(*subset)
                total_cantidad = sum(cantidades)
                total_valor = sum(precios)
                promedio_cantidad = total_cantidad / len(subset)
                valor_promedio_compra = total_valor / len(subset)
                # Valor unitario promedio = promedio de precios unitarios individuales
                valor_unitario_promedio = sum(p / c for p, c, _ in subset) / len(subset)
                valor_unitario_maximo = max(p / c for p, c, _ in subset)
                valor_unitario_minimo = min(p / c for p, c, _ in subset)
                meses = max(1, (max(fechas) - min(fechas)).days // 30)
                consumo_mensual = total_cantidad / meses

                def formatear(valor):
                    return f"{valor:.1f}" if valor < 100 else f"{valor:.0f}"

                st.markdown(f"### Unidad: **{unidad}**")
                st.markdown("| Métrica | Valor |")
                st.markdown("|--------|--------|")
                st.markdown(f"| Consumo promedio mensual | {formatear(consumo_mensual)} {unidad} |")
                st.markdown(f"| Compra promedio | {formatear(promedio_cantidad)} {unidad} |")
                st.markdown(f"| Valor promedio compra | {formatear(valor_promedio_compra)} $ |")
                st.markdown(f"| Valor unitario promedio | {formatear(valor_unitario_promedio)} $/{unidad} |")
                st.markdown(f"| Valor unitario máximo | {formatear(valor_unitario_maximo)} $/{unidad} |")
                st.markdown(f"| Valor unitario mínimo | {formatear(valor_unitario_minimo)} $/{unidad} |")

                st.markdown("#### Calculadora de compras")
                cant = st.number_input(f"¿Cuánta cantidad quieres comprar? ({unidad})", min_value=0.0, key=f"calc_{unidad}")
                if cant > 0:
                    valor_estimado = cant * valor_unitario_promedio
                    st.info(f"Valor referencial estimado: **${formatear(valor_estimado)}** por {cant} {unidad}")
    else:
        st.info("Ingresa precio, cantidad y unidad para todas las compras mostradas.")
else:
    st.info("Ingresa un producto para comenzar.")
