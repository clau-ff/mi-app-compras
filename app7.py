# Versión 3 de la App Compras Familiares (app7.py)
# Mejora: cantidad de registros dinámica, opción de descartar y agregar unidad

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

st.title("App Compras Familiares v3")
PASSWORD = st.secrets["CLAVE_FAMILIAR"]
password_input = st.text_input("Ingresa la clave familiar", type="password")
if password_input != PASSWORD:
    st.warning("Clave incorrecta o pendiente de ingresar.")
    st.stop()
st.success("¡Bienvenida/o!")

# --- Google Drive y Sheets ---
if not "SERVICE_ACCOUNT_JSON" in st.secrets:
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

# --- Buscar archivo Bluecoins más reciente ---
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
downloader = MediaIoBaseDownload(fh, request)
done = False
while done is False:
    status, done = downloader.next_chunk()
fh.close()
fecha_modif = latest_file['modifiedTime']
st.info(f"Archivo descargado: {latest_file['name']} (última modificación: {fecha_modif.replace('T', ' ').replace('Z','')})")

# --- Manejo base de datos ---
@st.cache_data
def leer_tabla(nombre_tabla):
    with sqlite3.connect("bluecoins.fydb") as conn:
        df = pd.read_sql_query(f"SELECT * FROM {nombre_tabla}", conn)
    return df

df_trans = leer_tabla("TRANSACTIONSTABLE")
df_trans['date'] = pd.to_datetime(df_trans['date'], errors='coerce')
df_trans = df_trans[df_trans['date'] <= pd.Timestamp(datetime.now().date())]
df_item = leer_tabla("ITEMTABLE")
df_pic = leer_tabla("PICTURETABLE")

# --- Búsqueda flexible ---
nombre_producto = st.text_input("Producto a buscar:")
if nombre_producto:
    if "descartados" not in st.session_state:
        st.session_state.descartados = set()

    trans_ids_con_boleta = set(df_pic['transactionID'].unique())
    df_trans_boleta = df_trans[df_trans['transactionsTableID'].isin(trans_ids_con_boleta)].copy()
    exactos = df_trans_boleta[df_trans_boleta['notes'].str.contains(nombre_producto, case=False, na=False)]
    exactos = exactos.sort_values("date", ascending=False)

    n_resultados = st.number_input("¿Cuántas compras quieres ver?", min_value=3, max_value=20, value=3)
    top = exactos.head(n_resultados)
    ids_ya = set(top['transactionsTableID']) | st.session_state.descartados

    if len(top) < n_resultados:
        resto = df_trans_boleta[~df_trans_boleta['transactionsTableID'].isin(ids_ya)].copy()
        resto['score'] = resto['notes'].astype(str).apply(lambda x: fuzz.partial_ratio(nombre_producto.lower(), x.lower()))
        resto = resto[resto['score'] > 70].sort_values("score", ascending=False)
        top = pd.concat([top, resto.head(n_resultados - len(top))])

    top = top.sort_values("date", ascending=False)
    trans_ids_mostradas = set()

    for idx, row in top.iterrows():
        tid = row['transactionsTableID']
        if tid in st.session_state.descartados:
            continue
        trans_ids_mostradas.add(str(tid))

        st.markdown(f"**Fecha:** {row['date'].strftime('%Y-%m-%d')} - {row['notes']}")
        if st.button("Descartar", key=f"descartar_{tid}"):
            st.session_state.descartados.add(tid)
            st.rerun()

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
                        comercio, '', precio, cantidad, unidad
                    ])
                    st.success("¡Compra registrada!")

    # --- Análisis ---
    registros_sheet = set(ids_existentes)
    if trans_ids_mostradas.issubset(registros_sheet):
        datos = [
            (float(f['Precio']), float(f['Cantidad']), f.get('Unidad', ''), pd.to_datetime(f['Fecha'], errors='coerce'))
            for f in filas if f['Producto buscado'].lower() == nombre_producto.lower()
        ]
        if datos:
            st.subheader("Resumen por unidad:")
            for unidad in set(u for _, _, u, _ in datos if u):
                subset = [(p, c, d) for p, c, u2, d in datos if u2 == unidad]
                total = sum(p*c for p, c, _ in subset)
                total_cant = sum(c for _, c, _ in subset)
                consumo_mensual = total_cant / max(1, ((max(d for _, _, d in subset) - min(d for _, _, d in subset)).days // 30))
                st.markdown(f"- Unidad **{unidad}**: Precio prom: {total/total_cant:.2f}, Consumo mensual: {consumo_mensual:.2f}")
        else:
            st.info("No hay datos suficientes para análisis.")
else:
    st.info("Ingresa un producto para comenzar.")
