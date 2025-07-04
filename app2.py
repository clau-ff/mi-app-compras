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
    df_trans['fuzzy_score'] = df_trans['notes'].astype(str).apply(lambda x: fuzz.partial_ratio(nombre_producto.lower(), x.lower()))
    df_filtrado = df_trans[df_trans['fuzzy_score'] > 70].sort_values("date", ascending=False)
    top3 = df_filtrado.head(3)

    if top3.empty:
        st.warning("No se encontraron compras similares.")
    else:
        st.subheader("Últimas 3 compras encontradas:")
        for idx, row in top3.iterrows():
            st.markdown(f"- **Fecha:** {row['date']}")
            st.markdown(f"  - **Nota:** {row['notes']}")
            # Buscar nombre de comercio
            comercio = None
            if 'itemID' in row and not pd.isnull(row['itemID']):
                item_row = df_item[df_item['itemTableID'] == row['itemID']]
                if not item_row.empty:
                    comercio = item_row.iloc[0]['itemName']
            if comercio:
                st.markdown(f"  - **Lugar/Comercio:** {comercio}")
            # Buscar imagen
            pic = df_pic[df_pic['transactionID'] == row['transactionsTableID']]
            if not pic.empty:
                archivo_img = pic.iloc[0]['pictureFileName']
                st.markdown(f"  - Archivo imagen: {archivo_img}")
            st.markdown("---")
else:
    st.info("Por favor, ingresa el nombre del producto a buscar.")
