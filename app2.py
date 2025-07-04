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
nombre_producto = st.text_input("Escribe el nombre del producto que quieres buscar:")

# Cuando la usuaria escriba el nombre y presione Enter, se ejecuta la búsqueda flexible (fuzzy matching) sobre 'notes'
if nombre_producto:
    st.write(f"Buscando compras de: {nombre_producto} (coincidencia flexible)...")
    # Aquí se cargará el DataFrame con las compras y se buscarán coincidencias usando fuzzywuzzy

    # 5. MOSTRAR ÚLTIMAS 3 COMPRAS RELACIONADAS
    st.subheader("Últimas 3 compras de este producto:")
    # Mostrar fecha, nota escrita, lugar (desde ITEMTABLE) e imagen boleta (desde PICTURETABLE y carpeta de Drive)
    # -- Aquí irá el código para buscar y mostrar esos datos --

    # 6. INGRESO MANUAL DE PRECIO Y CANTIDAD
    st.subheader("Registrar nueva compra (manual)")
    with st.form("registro_manual"):
        precio = st.number_input("Precio pagado", min_value=0.0, format="%.2f")
        cantidad = st.number_input("Cantidad comprada", min_value=1, step=1)
        submit = st.form_submit_button("Registrar en Google Sheets")

    if submit:
        st.write("Registrando compra... (Aquí se agregará a Google Sheets, sin duplicar transacciones)")

    # 7. ANÁLISIS SIMPLE DE PRECIOS
    st.subheader("¿Conviene comprar ahora?")
    st.write("Precio mínimo y promedio histórico: (Aquí se mostrarán los cálculos)")

    # 8. RECOMENDACIÓN DE CANTIDAD
    st.subheader("Sugerencia de cantidad a comprar")
    st.write("Aquí se recomendará cuántas unidades comprar si está barato.")

else:
    st.info("Por favor, ingresa el nombre del producto a buscar.")

st.caption("""
⚙️ Próximos pasos automáticos de la app:
- La app buscará coincidencias en tu historial, te mostrará las últimas 3 compras (con foto de boleta) y te permitirá registrar manualmente si es necesario.
- Los análisis y sugerencias se calculan automáticamente al mostrar las compras.
""")
