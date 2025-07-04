# Función mostrar información de compra e imagen legible

from PIL import Image, ExifTags

def mostrar_info_compra(row, comercio, archivo_img, ruta_local_img):
    st.markdown(f"- **Fecha:** {row['date']}")
    st.markdown(f"  - **Nota:** {row['notes']}")
    if comercio:
        st.markdown(f"  - **Lugar/Comercio:** {comercio}")
    if archivo_img:
        st.markdown(f"  - **Nombre archivo imagen:** {archivo_img}")

    # Mostrar imagen con rotación automática si es necesario y en buena resolución
    if ruta_local_img:
        imagen = Image.open(ruta_local_img)
        # Intentar rotar según EXIF (muy útil para fotos de móvil)
        try:
            for orientation in ExifTags.TAGS.keys():
                if ExifTags.TAGS[orientation] == 'Orientation':
                    break
            exif = imagen._getexif()
            if exif is not None:
                orientation_value = exif.get(orientation, None)
                if orientation_value == 3:
                    imagen = imagen.rotate(180, expand=True)
                elif orientation_value == 6:
                    imagen = imagen.rotate(270, expand=True)
                elif orientation_value == 8:
                    imagen = imagen.rotate(90, expand=True)
        except Exception as e:
            pass  # Si la imagen no tiene EXIF, no se rota
        # Ajustar tamaño para mejor lectura y forzar verticalidad
        width, height = imagen.size
        if width > height:
            imagen = imagen.rotate(90, expand=True)
        max_width = 700
        if imagen.size[0] > max_width:
            new_height = int(max_width * imagen.size[1] / imagen.size[0])
            imagen = imagen.resize((max_width, new_height))
        st.image(imagen, caption="Boleta", use_container_width=True)
        
# Función descargar_y_mostrar_imagen
import tempfile
from PIL import Image

def descargar_y_mostrar_imagen(drive_service, file_name, carpeta_id):
    # Busca el archivo por nombre dentro de la carpeta de fotos
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
    # Retorna la ruta local del archivo
    return tmp_file.name

import streamlit as st

# 1. LOGIN SIMPLE
st.title("App Compras Familiares")

PASSWORD = "ratas2025"
password_input = st.text_input("Ingresa la clave familiar", type="password")
if password_input != PASSWORD:
    st.warning("Clave incorrecta o pendiente de ingresar.")
    st.stop()

st.success("¡Bienvenida/o!")

import json
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ----- Crear archivo temporal con credenciales -----
if not "SERVICE_ACCOUNT_JSON" in st.secrets:
    st.error("No se encontró SERVICE_ACCOUNT_JSON en tus Secrets.")
    st.stop()

SERVICE_ACCOUNT_FILE = "service_account.json"
if not st.session_state.get("service_account_file_created", False):
    with open(SERVICE_ACCOUNT_FILE, "w") as f:
        f.write(st.secrets["SERVICE_ACCOUNT_JSON"])
    st.session_state["service_account_file_created"] = True

# ----- Definir SCOPES -----
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]

# ----- Autenticar -----
credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES
)

drive_service = build("drive", "v3", credentials=credentials)

# ----- Buscar carpeta 'Bluecoins/QuickSync' -----
def buscar_carpeta(nombre, parent_id=None):
    q = f"name = '{nombre}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        q += f" and '{parent_id}' in parents"
    results = drive_service.files().list(q=q, fields="files(id, name)").execute()
    files = results.get('files', [])
    if not files:
        return None
    return files[0]['id']

# Buscar la carpeta 'Bluecoins'
bluecoins_id = buscar_carpeta('Bluecoins')
if not bluecoins_id:
    st.error("No se encontró la carpeta 'Bluecoins'.")
    st.stop()

# Buscar la carpeta 'Pictures'
pictures_id = buscar_carpeta('Pictures', parent_id=bluecoins_id)
if not pictures_id:
    st.error("No se encontró la carpeta 'Pictures' dentro de 'Bluecoins'.")
    st.stop()

# Buscar la subcarpeta 'QuickSync'
quicksync_id = buscar_carpeta('QuickSync', parent_id=bluecoins_id)
if not quicksync_id:
    st.error("No se encontró la carpeta 'QuickSync' dentro de 'Bluecoins'.")
    st.stop()

# ----- Buscar archivos .fydb en QuickSync -----
q = f"'{quicksync_id}' in parents and trashed = false and name contains '.fydb'"
results = drive_service.files().list(q=q, fields="files(id, name, modifiedTime)").execute()
fydb_files = results.get('files', [])
if not fydb_files:
    st.error("No se encontraron archivos .fydb en la carpeta QuickSync.")
    st.stop()

# ----- Encontrar el más reciente -----
fydb_files.sort(key=lambda x: x['modifiedTime'], reverse=True)
latest_file = fydb_files[0]
st.success(f"Archivo Bluecoins más reciente: {latest_file['name']} (modificado: {latest_file['modifiedTime']})")

# ----- Descargar el archivo a la app -----
request = drive_service.files().get_media(fileId=latest_file['id'])
import io
from googleapiclient.http import MediaIoBaseDownload
fh = io.FileIO("bluecoins.fydb", "wb")
downloader = MediaIoBaseDownload(fh, request)
done = False
while done is False:
    status, done = downloader.next_chunk()
fh.close()
st.info(f"Archivo descargado como bluecoins.fydb en la app.")

st.header("Buscar producto en historial de compras")

# 4. BUSCAR PRODUCTO

import sqlite3
import pandas as pd
from fuzzywuzzy import fuzz

DB_FILENAME = "bluecoins.fydb"

@st.cache_data
def leer_tabla(nombre_tabla):
    with sqlite3.connect(DB_FILENAME) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {nombre_tabla}", conn)
    return df

# Lee las tablas principales
df_trans = leer_tabla("TRANSACTIONSTABLE")
df_item = leer_tabla("ITEMTABLE")
df_pic = leer_tabla("PICTURETABLE")

# Búsqueda flexible
nombre_producto = st.text_input("Escribe el nombre del producto que quieres buscar:")

if nombre_producto:
    df_filtrado = df_trans[df_trans['fuzzy_score'] > 70]
    df_filtrado = df_filtrado.sort_values("date", ascending=False)
    df_filtrado['date'] = pd.to_datetime(df_filtrado['date'], errors='coerce')
    df_filtrado = df_filtrado.sort_values("date", ascending=False)
    top3 = df_filtrado.head(3)

    if top3.empty:
        st.warning("No se encontraron compras similares.")
    else:
        st.subheader("Últimas 3 compras encontradas:")
    for idx, row in top3.iterrows():
        # Buscar comercio
        comercio = None
        if 'itemID' in row and not pd.isnull(row['itemID']):
            item_row = df_item[df_item['itemTableID'] == row['itemID']]
            if not item_row.empty:
                comercio = item_row.iloc[0]['itemName']

    # Buscar imagen
    archivo_img = None
    ruta_local = None
    pic = df_pic[df_pic['transactionID'] == row['transactionsTableID']]
    if not pic.empty:
        archivo_img = pic.iloc[0]['pictureFileName']
        ruta_local = descargar_y_mostrar_imagen(drive_service, archivo_img, pictures_id)

    # Mostrar datos siempre, imagen sólo si hay archivo
    mostrar_info_compra(row, comercio, archivo_img, ruta_local)

    # Mostrar formulario solo si hay imagen
    if ruta_local:
        with st.form(f"form_{row['transactionsTableID']}"):
            precio = st.number_input("Precio pagado", min_value=0.0, format="%.2f", key=f"precio_{row['transactionsTableID']}")
            cantidad = st.number_input("Cantidad comprada", min_value=1, step=1, key=f"cantidad_{row['transactionsTableID']}")
            submit = st.form_submit_button("Registrar en Google Sheets")
        if submit:
            st.success("¡Compra registrada! (esto después se enviará a Google Sheets)")

    st.markdown("---")
else:
    st.info("Por favor, ingresa el nombre del producto a buscar.")
