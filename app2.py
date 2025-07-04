import streamlit as st

# 1. LOGIN SIMPLE
st.title("App Compras Familiares")

PASSWORD = "ratas2025"
password_input = st.text_input("Ingresa la clave familiar", type="password")
if password_input != PASSWORD:
    st.warning("Clave incorrecta o pendiente de ingresar.")
    st.stop()

st.success("¡Bienvenida/o!")

import os

# 1.1 Crear archivo client_secrets.json temporalmente si no existe
if not os.path.exists("client_secrets.json"):
    with open("client_secrets.json", "w") as f:
        f.write(st.secrets["CLIENT_SECRETS"])

# 2. AUTORIZACIÓN GOOGLE DRIVE Y SHEETS (próximo paso)
st.info("Haz clic para autorizar acceso temporal a Google Drive y Sheets. Esto es seguro y privado para tu familia.")

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from datetime import datetime

# ---- AUTENTICACIÓN GOOGLE DRIVE ----
st.subheader("Autoriza acceso a tu Google Drive")

if "drive_auth_ok" not in st.session_state:
    gauth = GoogleAuth()
    gauth.DEFAULT_SETTINGS['client_config_file'] = "client_secrets.json"
    gauth.CommandLineAuth()  # AUTORIZACIÓN POR LINK Y CÓDIGO (funciona en Streamlit Cloud)
    st.session_state["gauth"] = gauth
    st.session_state["drive_auth_ok"] = True
    st.success("¡Acceso a Google Drive autorizado!")
else:
    gauth = st.session_state["gauth"]

drive = GoogleDrive(gauth)

# ---- BUSCAR ARCHIVO BLUECOINS MÁS RECIENTE EN LA CARPETA ----
# Asume que tu carpeta destino es: MyDrive/Bluecoins/QuickSync/
st.info("Buscando archivo Bluecoins más reciente en /Bluecoins/QuickSync/…")

# 1. Buscar la carpeta 'Bluecoins'
bluecoins_folders = drive.ListFile({
    'q': "title='Bluecoins' and mimeType='application/vnd.google-apps.folder' and trashed=false"
}).GetList()
if not bluecoins_folders:
    st.error("No se encontró la carpeta 'Bluecoins' en tu Drive.")
    st.stop()
bluecoins_folder_id = bluecoins_folders[0]['id']

# 2. Buscar la subcarpeta 'QuickSync'
quicksync_folders = drive.ListFile({
    'q': f"'{bluecoins_folder_id}' in parents and title='QuickSync' and mimeType='application/vnd.google-apps.folder' and trashed=false"
}).GetList()
if not quicksync_folders:
    st.error("No se encontró la carpeta 'QuickSync' dentro de 'Bluecoins'.")
    st.stop()
quicksync_folder_id = quicksync_folders[0]['id']

# 3. Buscar los archivos .fydb dentro de esa carpeta
fydb_files = drive.ListFile({
    'q': f"'{quicksync_folder_id}' in parents and trashed=false and title contains '.fydb'"
}).GetList()
if not fydb_files:
    st.error("No se encontraron archivos .fydb en la carpeta QuickSync.")
    st.stop()

# 4. Buscar el más reciente por fecha de modificación
latest_file = max(fydb_files, key=lambda x: x['modifiedDate'])

# 5. Descargar el archivo
dest_filename = "bluecoins.fydb"
latest_file.GetContentFile(dest_filename)
st.success(f"Archivo Bluecoins descargado: {latest_file['title']} (última modificación: {latest_file['modifiedDate']})")

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
