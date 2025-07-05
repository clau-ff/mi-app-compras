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
import os
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
st.info(f"Archivo descargado: {latest_file['name']}")

# Leer tablas de SQLite
DB_FILENAME = "bluecoins.fydb"
@st.cache_data
def leer_tabla(nombre_tabla):
    with sqlite3.connect(DB_FILENAME) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {nombre_tabla}", conn)
    return df

df_trans = leer_tabla("TRANSACTIONSTABLE")
df_item = leer_tabla("ITEMTABLE")
df_pic = leer_tabla("PICTURETABLE")

# ---- Búsqueda flexible de producto ----
nombre_producto = st.text_input("Escribe el nombre del producto que quieres buscar:", key="nombre_producto")

if nombre_producto:
    df_trans['fuzzy_score'] = df_trans['notes'].astype(str).apply(lambda x: fuzz.partial_ratio(nombre_producto.lower(), x.lower()))
    df_filtrado = df_trans[df_trans['fuzzy_score'] > 70].copy()
    # Asegurarse que la columna fecha es datetime para ordenar
    df_filtrado['date'] = pd.to_datetime(df_filtrado['date'], errors='coerce')
    df_filtrado = df_filtrado.sort_values("date", ascending=False)
    top3 = df_filtrado.head(3)

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
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.close()
        return tmp_file.name

    from PIL import Image, ExifTags
    
    SHEETS_ID = st.secrets["SHEETS_ID"]
    gc = gspread.authorize(credentials)
    worksheet = gc.open_by_key(SHEETS_ID).sheet1  # usa la hoja 1

    for idx, row in top3.iterrows():
        # ... (tus búsquedas de comercio, archivo_img y ruta_local) ...
        # ----- Buscar comercio -----
        comercio = None
        if 'itemID' in row and not pd.isnull(row['itemID']):
            item_row = df_item[df_item['itemTableID'] == row['itemID']]
            if not item_row.empty:
                comercio = item_row.iloc[0]['itemName']
                
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
                # PREVISUALIZAR PDF
                with open(ruta_local, "rb") as f:
                    base64_pdf = base64.b64encode(f.read()).decode('utf-8')
                pdf_display = f"""
                    <iframe src="data:application/pdf;base64,{base64_pdf}" width="700" height="900" type="application/pdf"></iframe>
                """
                st.components.v1.html(pdf_display, height=920)
                mostrar_formulario = True  # AHORA SIEMPRE muestra el formulario aunque sea PDF
            else:
                st.warning("Formato de archivo no soportado para previsualización.")
                mostrar_formulario = False
        else:
            st.info("No hay archivo de boleta disponible.")

            # Formulario SOLO si hay imagen
        if mostrar_formulario:
            with st.form(f"form_{row['transactionsTableID']}"):
                precio = st.number_input("Precio pagado", min_value=0.0, format="%.2f", key=f"precio_{row['transactionsTableID']}")
                cantidad = st.number_input("Cantidad comprada", min_value=1, step=1, key=f"cantidad_{row['transactionsTableID']}")
                submit = st.form_submit_button("Registrar en Google Sheets")
            if submit:
                registros = worksheet.col_values(1)
                if str(row['transactionsTableID']) not in registros:
                    worksheet.append_row([
                        str(row['transactionsTableID']),
                        row['date'].strftime('%Y-%m-%d') if not pd.isnull(row['date']) else row['date'],
                        row['notes'],
                        comercio if comercio else '',
                        archivo_img if archivo_img else '',
                        precio,
                        cantidad
                    ])
                    st.success("¡Compra guardada en Google Sheets!")
                else:
                    st.info("Ya existe un registro para esta transacción en Sheets, no se duplicó.")
        st.markdown("---")
else:
    st.info("Por favor, ingresa el nombre del producto a buscar.")
