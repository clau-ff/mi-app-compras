# -*- coding: utf-8 -*-
# App Compras Familiares (v4) - archivo limpio y coherente
# Mantiene funcionalidades existentes:
# - Acceso por clave
# - Descarga del .fydb más reciente desde Drive (Bluecoins/QuickSync)
# - Búsqueda SOLO en TRANSACTIONSTABLE.notes (con normalización + fuzzy fallback)
# - Agrupación por componente (closure split) con NewSplitTransactionID (DSU)
# - Filtro "Sólo con boleta" a nivel de componente
# - Muestra N compras como MÁXIMO (no mínimo) ordenadas por boleta más reciente
# - Muestra Nota antes de boletas
# - Botón "Descartar" por componente
# - Guardado en Google Sheets (HistorialCompras) con:
#   * registro único por (Producto buscado, transactionsTableID)
#   * update si existe el par, append si no existe
#   * auditoría: columna "Última modificación" (J)
# - Resumen (min/max/prom y consumo mensual) en cabecera, justo después del artículo y antes del loop de boletas
# - Panel DEBUG opcional

import base64
import os
import sqlite3
import tempfile
import unicodedata
from datetime import datetime

import gspread
import pandas as pd
import streamlit as st
import time
from fuzzywuzzy import fuzz
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from PIL import Image

DEBUG = False

st.title("App Compras Familiares v4")

# Flash message post-rerun
if "flash_ok" not in st.session_state:
    st.session_state.flash_ok = None

if st.session_state.flash_ok:
    st.success(st.session_state.flash_ok)
    st.session_state.flash_ok = None

# -------------------------
# Utilidades
# -------------------------
def normalizar(texto) -> str:
    if not isinstance(texto, str):
        return ""
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("utf-8")
    return texto.lower().strip()


def parse_bluecoins_datetime(series: pd.Series) -> pd.Series:
    # La base trae "YYYY-MM-DD HH:MM:SS.0" → remover ".0"
    cleaned = series.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    return pd.to_datetime(cleaned, format="%Y-%m-%d %H:%M:%S", errors="coerce")


def to_float(x):
    try:
        s = str(x).strip()
        if s == "":
            return None
        # tolerante a separadores
        s = s.replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return None

def formatear_pesos(valor):
    try:
        return f"${valor:,.0f}".replace(",", ".")
    except:
        return "$0"

# -------------------------
# Acceso
# -------------------------
PASSWORD = st.secrets["CLAVE_FAMILIAR"]
password_input = st.text_input("Ingresa la clave familiar", type="password")
if password_input != PASSWORD:
    st.warning("Clave incorrecta o pendiente de ingresar.")
    st.stop()
st.success("¡Bienvenida/o!")


# -------------------------
# Google Drive / Sheets
# -------------------------
if "SERVICE_ACCOUNT_JSON" not in st.secrets:
    st.error("No se encontró SERVICE_ACCOUNT_JSON en tus Secrets.")
    st.stop()

SERVICE_ACCOUNT_FILE = "service_account.json"
if not st.session_state.get("service_account_file_created", False):
    with open(SERVICE_ACCOUNT_FILE, "w", encoding="utf-8") as f:
        f.write(st.secrets["SERVICE_ACCOUNT_JSON"])
    st.session_state["service_account_file_created"] = True

SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build("drive", "v3", credentials=credentials)
gc = gspread.authorize(credentials)
worksheet = gc.open_by_key(st.secrets["SHEETS_ID"]).sheet1


# -------------------------
# DEBUG
# -------------------------
DEBUG = st.sidebar.checkbox("DEBUG", value=False)


def dbg(title: str, obj):
    if DEBUG:
        st.sidebar.markdown(f"**{title}**")
        st.sidebar.write(obj)


def dbg_df(title: str, df: pd.DataFrame, n: int = 50):
    if DEBUG:
        st.sidebar.markdown(f"**{title} (top {n})**")
        st.sidebar.dataframe(df.head(n))


# -------------------------
# Sheets helpers (HistorialCompras)
# -------------------------
# --- 1. CARGAR HISTORIAL (Mejorado para identificar filas exactas) ---
@st.cache_data(ttl=60)
def cargar_historial(_worksheet):
    values = _worksheet.get_all_values()
    if not values or len(values) < 2:
        return pd.DataFrame(), {}

    headers = values[0]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=headers)
    # Guardamos la fila real de Sheets (indice 0 es fila 1, por eso sumamos 2)
    df["_row_number"] = range(2, 2 + len(df)) 

    if "Producto buscado" in df.columns:
        df["prod_norm"] = df["Producto buscado"].apply(normalizar)
    else:
        df["prod_norm"] = ""

    # Creamos un diccionario de búsqueda rápida: (producto_norm, tid) -> fila_sheets
    row_by_key = {}
    if "Producto buscado" in df.columns and "transactionsTableID" in df.columns:
        for idx, row in df.iterrows():
            p_norm = normalizar(row["Producto buscado"])
            tid_str = str(row["transactionsTableID"]).strip()
            if tid_str:
                row_by_key[(p_norm, tid_str)] = int(row["_row_number"])

    return df, row_by_key

# --- 2. CÁLCULO DE RESUMEN (Promedio Ponderado y Consumo Mensual) ---
def calcular_resumen(df_hist_prod: pd.DataFrame, unidad: str):
    if df_hist_prod.empty: return None
    
    unidad = str(unidad).strip()
    sub = df_hist_prod[df_hist_prod["Unidad"].astype(str).str.strip() == unidad].copy()
    
    sub["Precio_f"] = sub["Precio"].apply(to_float)
    sub["Cantidad_f"] = sub["Cantidad"].apply(to_float)
    sub["Fecha_dt"] = pd.to_datetime(sub["Fecha"], errors="coerce")
    sub = sub.dropna(subset=["Precio_f", "Cantidad_f", "Fecha_dt"])
    sub = sub[(sub["Precio_f"] > 0) & (sub["Cantidad_f"] > 0)]
    
    if sub.empty: return None

    # Métricas solicitadas
    precio_min = float(sub["Precio_f"].min())
    precio_max = float(sub["Precio_f"].max())
    
    # Promedio Ponderado: sum(P*C) / sum(C)
    suma_pc = (sub["Precio_f"] * sub["Cantidad_f"]).sum()
    suma_c = sub["Cantidad_f"].sum()
    precio_prom_ponderado = suma_pc / suma_c

    # Consumo Mensual: sum(Cant) / (rango dias / 30.44)
    f_ini, f_fin = sub["Fecha_dt"].min(), sub["Fecha_dt"].max()
    dias = max(1, (f_fin - f_ini).days)
    meses = dias / 30.44
    consumo_mensual = suma_c / max(0.03, meses) # Evitar division por casi cero

    return {
        "unidad": unidad,
        "periodo_ini": f_ini,
        "periodo_fin": f_fin,
        "compras": len(sub),
        "precio_min": precio_min,
        "precio_max": precio_max,
        "precio_prom": precio_prom_ponderado,
        "consumo_mensual": consumo_mensual
    }

# -------------------------
# Drive helpers
# -------------------------
def buscar_carpeta(nombre: str, parent_id: str | None = None) -> str | None:
    q = f"name = '{nombre}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        q += f" and '{parent_id}' in parents"
    results = drive_service.files().list(q=q, fields="files(id, name)").execute()
    files = results.get("files", [])
    return files[0]["id"] if files else None


bluecoins_id = buscar_carpeta("Bluecoins")
quicksync_id = buscar_carpeta("QuickSync", parent_id=bluecoins_id) if bluecoins_id else None
pictures_id = buscar_carpeta("Pictures", parent_id=bluecoins_id) if bluecoins_id else None

if not bluecoins_id or not quicksync_id or not pictures_id:
    st.error("No se encontraron carpetas requeridas en Drive (Bluecoins/QuickSync/Pictures).")
    st.stop()


def descargar_archivo_boleta(file_name: str) -> str | None:
    q = f"'{pictures_id}' in parents and trashed = false and name = '{file_name}'"
    results = drive_service.files().list(q=q, fields="files(id, name)").execute()
    files = results.get("files", [])
    if not files:
        return None
    file_id = files[0]["id"]
    request = drive_service.files().get_media(fileId=file_id)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix="." + file_name.split(".")[-1])
    with open(tmp.name, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return tmp.name


def mostrar_boleta_desde_ruta(ruta: str, key_suffix: str):
    ext = os.path.splitext(ruta)[-1].lower()
    if ext in [".jpg", ".jpeg", ".png"]:
        imagen = Image.open(ruta)
        if imagen.width > imagen.height:
            imagen = imagen.rotate(90, expand=True)
        st.image(imagen, caption="Boleta")
    elif ext == ".pdf":
        with open(ruta, "rb") as f:
            base64_pdf = base64.b64encode(f.read()).decode("utf-8")
        pdf_display = f"""
            <iframe src="data:application/pdf;base64,{base64_pdf}"
                    width="700" height="900" type="application/pdf"></iframe>
        """
        st.components.v1.html(pdf_display, height=920)
        with open(ruta, "rb") as f:
            st.download_button(
                label="Descargar boleta PDF",
                data=f,
                file_name=os.path.basename(ruta),
                mime="application/pdf",
                key=f"dlpdf_{key_suffix}",
            )


# -------------------------
# Descargar .fydb más reciente
# -------------------------
q = f"'{quicksync_id}' in parents and trashed = false and name contains '.fydb'"
results = drive_service.files().list(q=q, fields="files(id, name, modifiedTime)").execute()
fydb_files = results.get("files", [])
if not fydb_files:
    st.error("No se encontraron archivos .fydb en la carpeta QuickSync.")
    st.stop()

fydb_files.sort(key=lambda x: x["modifiedTime"], reverse=True)
latest_file = fydb_files[0]

request = drive_service.files().get_media(fileId=latest_file["id"])
with open("bluecoins.fydb", "wb") as fh:
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

fecha_modif = latest_file["modifiedTime"]
st.info(
    f"Archivo descargado: {latest_file['name']} "
    f"(última modificación: {fecha_modif.replace('T', ' ').replace('Z', '')})"
)


# -------------------------
# Lectura de tablas SQLite
# -------------------------
@st.cache_data
def leer_tabla(nombre_tabla: str) -> pd.DataFrame:
    with sqlite3.connect("bluecoins.fydb") as conn:
        return pd.read_sql_query(f"SELECT * FROM {nombre_tabla}", conn)


df_trans = leer_tabla("TRANSACTIONSTABLE")
df_trans["date"] = parse_bluecoins_datetime(df_trans["date"])
df_trans = df_trans[df_trans["date"] <= pd.Timestamp(datetime.now().date())].copy()

df_item = leer_tabla("ITEMTABLE")  # solo para "comercio" al guardar
df_pic = leer_tabla("PICTURETABLE")


# -------------------------
# UI búsqueda
# -------------------------
nombre_producto = st.text_input("Producto a buscar:")
solo_con_boleta = st.radio("¿Mostrar sólo registros con boleta?", options=["Sí", "No"], index=0)
n_resultados = st.number_input("¿Cuántas compras quieres ver? (máximo)", min_value=1, max_value=50, value=3)

if not nombre_producto:
    st.info("Ingresa un producto para comenzar.")
    st.stop()

# Estado descartados (por componente)
if "descartados_comp" not in st.session_state:
    st.session_state.descartados_comp = set()

nombre_normalizado = normalizar(nombre_producto)

# Historial en memoria
df_hist, row_by_key = cargar_historial(worksheet)

# --- RESUMEN EN CABECERA (CORREGIDO) ---
hist_prod = df_hist[df_hist["prod_norm"] == nombre_normalizado].copy()

if not hist_prod.empty:
    # 1. Determinamos la unidad más frecuente para este producto en el historial
    u_counts = hist_prod["Unidad"].astype(str).str.strip()
    u_counts = u_counts[u_counts != ""]
    
    if not u_counts.empty:
        unidad_para_resumen = u_counts.value_counts().idxmax()
        
        # 2. Calculamos el resumen usando esa unidad
        res = calcular_resumen(hist_prod, unidad_para_resumen)
        
        if res:
# NUEVO: consumo mensual sin la última compra
# 1. Filtrar historial por unidad usada
            df_filtrado = hist_prod[hist_prod["Unidad"].astype(str).str.strip() == unidad_para_resumen].copy()
            df_filtrado["Cantidad_f"] = df_filtrado["Cantidad"].apply(to_float)
            df_filtrado["Fecha_dt"] = pd.to_datetime(df_filtrado["Fecha"], errors="coerce")
            df_filtrado = df_filtrado.dropna(subset=["Cantidad_f", "Fecha_dt"])
            df_filtrado = df_filtrado[(df_filtrado["Cantidad_f"] > 0)]

# 2. Ordenar y separar última compra
            df_ordenado = df_filtrado.sort_values("Fecha_dt")
            df_consumo = df_ordenado.iloc[:-1] if len(df_ordenado) > 1 else pd.DataFrame()

# 3. Cálculo de unidades consumidas y consumo mensual promedio
            unidades_consumidas = df_consumo["Cantidad_f"].sum() if not df_consumo.empty else 0.0
# Mostrar unidades consumidas y rango completo (incluye fecha de la última compra)
            if not df_consumo.empty:
                f_ini = df_consumo["Fecha_dt"].min()
                f_fin = df_ordenado["Fecha_dt"].max() # ← mantiene el rango completo
                dias = max(1, (f_fin - f_ini).days)
                consumo_mensual_ajustado = unidades_consumidas / (dias / 30)
            else:
                f_ini = f_fin = pd.NaT
                consumo_mensual_ajustado = 0.0

# 4. Mostrar panel visual
            st.markdown(f"<div style='background-color:#e6f4ea; padding:0.6em; border-radius:0.5em;'>"
            f"<b>📊 Resumen Histórico Global ({res['unidad']})</b></div>",
            unsafe_allow_html=True)

            col1, col2 = st.columns(2)
            with col1:
                st.markdown(
                    f"""<div>
                    <b>Rango:</b> {res['periodo_ini'].strftime('%d/%m/%y')} al {res['periodo_fin'].strftime('%d/%m/%y')}<br>
                    <b>Unidades consumidas:</b> {unidades_consumidas:.2f}
                    </div>""",
                    unsafe_allow_html=True
                )

            with col2:
                st.markdown(
                    f"""<div>
                    <b>Precio Unitario:</b><br>
                    <span style='margin-left: 10px;'>Min: <span style='color: green; font-family: monospace;'>{formatear_pesos(res['precio_min'])}</span></span><br>
                    <span style='margin-left: 10px;'>Máx: <span style='color: red; font-family: monospace;'>{formatear_pesos(res['precio_max'])}</span></span><br>
                    <span style='margin-left: 10px;'>Prom. Ponderado: <span style='color: blue; font-family: monospace;'>{formatear_pesos(res['precio_prom'])}</span></span>
                    </div>""",
                    unsafe_allow_html=True
                )

            st.markdown(
                f"""<div style="background-color:#eaf4fc; padding: 0.75em; border-radius: 0.5em;">
                    💡 <b>Consumo mensual promedio:</b>
                    <span style='font-family: monospace;'>{consumo_mensual_ajustado:.2f} {res['unidad']} / mes</span><br>
                    <small style='color: #555;'>Última compra no incluida en el consumo</small>
                </div>""",
                unsafe_allow_html=True
            )

    else:
        st.info("No hay unidades registradas en el historial para calcular el resumen.")
else:
    st.info("Aún no hay historial para este producto. Los datos aparecerán aquí una vez que guardes la primera compra.")

# -------------------------
# DSU para closure de split/hermanas
# -------------------------
class DSU:
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def add(self, x: str):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x: str) -> str:
        self.add(x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: str, b: str):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1


df_trans = df_trans.copy()
df_trans["tid_str"] = df_trans["transactionsTableID"].astype(str)

if "NewSplitTransactionID" in df_trans.columns:
    df_trans["split_str_raw"] = df_trans["NewSplitTransactionID"].astype(str)
    df_trans.loc[df_trans["NewSplitTransactionID"].isna(), "split_str_raw"] = ""
    df_trans["split_str_raw"] = df_trans["split_str_raw"].replace({"nan": "", "None": ""})
else:
    df_trans["split_str_raw"] = ""

df_trans["notes_norm"] = df_trans.get("notes", "").apply(normalizar)

dsu = DSU()
for t in df_trans["tid_str"].tolist():
    dsu.add(t)

for t, s in zip(df_trans["tid_str"].tolist(), df_trans["split_str_raw"].tolist()):
    if s:
        dsu.union(t, s)

# IDs con boleta
pic_ids = set()
if not df_pic.empty and "transactionID" in df_pic.columns:
    pic_ids = set(df_pic["transactionID"].astype(str).unique())
    for pid in pic_ids:
        dsu.add(pid)

df_trans["comp_id"] = df_trans["tid_str"].apply(dsu.find)

# componentes con boleta (a nivel comp_id)
comps_con_boleta = set(dsu.find(pid) for pid in pic_ids) if pic_ids else set()

df_base = df_trans.copy()

dbg("Inputs", {"producto": nombre_producto, "producto_norm": nombre_normalizado, "solo_con_boleta": solo_con_boleta, "n_resultados_max": int(n_resultados)})


# -------------------------
# Boletas por componente (ordenadas por fecha)
# -------------------------
def boletas_por_comp(df_pic_in: pd.DataFrame) -> pd.DataFrame:
    if df_pic_in.empty or "transactionID" not in df_pic_in.columns or "pictureFileName" not in df_pic_in.columns:
        return pd.DataFrame(columns=["comp_id", "pictureFileName", "_fecha", "tid_str"])

    aux = df_pic_in[["transactionID", "pictureFileName"]].copy()
    aux["tid_str"] = aux["transactionID"].astype(str)

    aux = aux.merge(df_trans[["tid_str", "date"]], on="tid_str", how="left")
    aux["_fecha"] = pd.to_datetime(aux["date"], errors="coerce")
    aux = aux.dropna(subset=["pictureFileName"])
    aux["comp_id"] = aux["tid_str"].apply(dsu.find)

    aux = aux.sort_values("_fecha", ascending=False)
    return aux


df_pic_comp = boletas_por_comp(df_pic)

file_to_fecha = {}
if not df_pic_comp.empty and {"pictureFileName", "_fecha"}.issubset(set(df_pic_comp.columns)):
    aux_ft = df_pic_comp.dropna(subset=["pictureFileName", "_fecha"]).copy()
    aux_ft = aux_ft.sort_values("_fecha", ascending=False).drop_duplicates(subset=["pictureFileName"], keep="first")
    file_to_fecha = dict(zip(aux_ft["pictureFileName"], aux_ft["_fecha"]))

comp_latest_date = {}
comp_to_files = {}
if not df_pic_comp.empty:
    comp_latest_date = df_pic_comp.groupby("comp_id")["_fecha"].max().to_dict()
    comp_to_files = df_pic_comp.groupby("comp_id")["pictureFileName"].apply(list).to_dict()


def orden_comp(comp_id: str) -> pd.Timestamp:
    return comp_latest_date.get(comp_id, pd.Timestamp.min)


# -------------------------
# TOP: 1 fila por componente
# -------------------------
def construir_top() -> pd.DataFrame:
    base_full = df_base[~df_base["comp_id"].isin(st.session_state.descartados_comp)].copy()

    matches = base_full[base_full["notes_norm"].str.contains(nombre_normalizado, na=False)].copy()

    if matches.empty and nombre_normalizado:
        cand = base_full[base_full["notes_norm"].str.len() > 0].copy()
        if not cand.empty:
            cand["score"] = cand["notes_norm"].apply(lambda x: fuzz.partial_ratio(nombre_normalizado, x))
            matches = cand[cand["score"] >= 80].copy()

    if matches.empty:
        return pd.DataFrame(columns=base_full.columns)

    if solo_con_boleta == "Sí":
        matches_con_boleta = matches[matches["comp_id"].isin(comps_con_boleta)].copy()
        if matches_con_boleta.empty and not matches.empty:
            st.warning(
                f"Se encontraron compras con '{nombre_producto}', pero ninguna tiene boleta. "
                f"Desactiva el filtro 'Sólo con boleta' para verlas."
            )
            return pd.DataFrame(columns=base_full.columns)
        matches = matches_con_boleta

    comps_sorted = sorted(matches["comp_id"].unique(), key=orden_comp, reverse=True)
    comps_top = comps_sorted[: int(n_resultados)]

    df_res = matches[matches["comp_id"].isin(comps_top)].copy()
    rank = {c: i for i, c in enumerate(comps_top)}
    df_res["__comp_rank"] = df_res["comp_id"].map(rank)
    df_res = df_res.sort_values(["__comp_rank", "date"], ascending=[True, False])
    return df_res.drop_duplicates(subset=["comp_id"], keep="first").drop(columns=["__comp_rank"])


top = construir_top()
if top.empty:
    st.warning("No se encontraron registros suficientes con los criterios seleccionados.")
    st.stop()


# -------------------------
# Render resultados
# -------------------------
trans_ids_mostradas = set()

for _, row in top.iterrows():
    tid = str(row["transactionsTableID"]).strip()
    comp_id = str(row["comp_id"])
    trans_ids_mostradas.add(tid)

    # Registro existente único por (producto, transacción)
    key_par = (normalizar(nombre_producto), tid)
    fila_existente = row_by_key.get(key_par)
    registro_existente = None
    if fila_existente and not df_hist.empty:
        aux_reg = df_hist[
            (df_hist["prod_norm"] == nombre_normalizado)
            & (df_hist["transactionsTableID"].astype(str).str.strip() == tid)
        ]
        if not aux_reg.empty:
            registro_existente = aux_reg.iloc[0].to_dict()

    # Defaults si existe
    precio_default = to_float(registro_existente.get("Precio")) if registro_existente else 0.0
    cantidad_default = to_float(registro_existente.get("Cantidad")) if registro_existente else 1.0
    unidad_default = (registro_existente.get("Unidad") or "").strip() if registro_existente else ""
     
    # UI: descartar por componente
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("Descartar", key=f"descartar_comp_{comp_id}"):
            st.session_state.descartados_comp.add(comp_id)
            st.rerun()

    with col2:

        # Nota antes de boletas
        st.markdown(f"**Nota:** {row.get('notes', '')}")

        # Boletas del componente (más recientes primero)
        files = comp_to_files.get(comp_id, [])
        if not files:
            st.info("No hay boleta asociada a esta compra (ni a sus transacciones hermanas).")
        else:
            for i, file_name in enumerate(files):
                fecha_file = file_to_fecha.get(file_name)
                if pd.notna(fecha_file):
                    st.markdown(f"**Fecha transacción:** {fecha_file.strftime('%d-%m-%Y %H:%M')}")
                else:
                    st.markdown("**Fecha transacción:** (sin fecha disponible)")

                ruta = descargar_archivo_boleta(file_name)
                if ruta:
                    mostrar_boleta_desde_ruta(ruta, key_suffix=f"{comp_id}_{i}")
                else:
                    st.info("Boleta referenciada pero no encontrada en Drive (Pictures).")

    # Archivo Imagen (boleta más reciente del componente)
    archivo_img = ""
    files = comp_to_files.get(comp_id, [])
    if files:
        archivo_img = files[0]

    # --- FORMULARIO DE GUARDADO CON VALIDACIÓN DE PAR ---
    
    actualizar_existente = False

    if DEBUG:
        st.sidebar.markdown("## DEBUG · Pre-form")
        st.sidebar.write({
            "comp_id": comp_id,
            "tid": tid,
            "fila_existente": fila_existente,
            "registro_existente?": bool(registro_existente),
        })
    actualizar = False

    with st.form(f"form_{comp_id}_{tid}"):
        actualizar_existente = actualizar

        fila_existente = row_by_key.get((nombre_normalizado, tid))
        
        if fila_existente:
            st.warning(f"⚠️ Ya existe registro para esta compra (Fila {fila_existente}).")
            actualizar = st.checkbox("Actualizar registro existente (Sobreescribir)", value=False, key=f"act_{comp_id}_{tid}")
        else:
            actualizar = False

        precio = st.number_input("Precio Unitario", value=float(precio_default), key=f"p_{tid}")
        cantidad = st.number_input("Cantidad", value=float(cantidad_default), key=f"c_{tid}")
        unidad = st.text_input("Unidad", value=unidad_default, key=f"u_{tid}")
        
        btn_label = "Actualizar Datos" if fila_existente else "Guardar Nueva Compra"
        if DEBUG:
            st.sidebar.markdown("## DEBUG · Form values (antes de submit)")
            st.sidebar.write({
            "precio": precio,
            "cantidad": cantidad,
            "unidad": unidad,
            "actualizar_existente": actualizar_existente,
        })

        submit = st.form_submit_button(btn_label)

    if submit:
    # ===== DEFINICIÓN OBLIGATORIA DE FILA (SIEMPRE) =====
        fecha_modificacion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        comercio = ""
        if not pd.isnull(row.get("itemID")):
            item_row = df_item[df_item["itemTableID"] == row["itemID"]]
            if not item_row.empty:
                comercio = item_row.iloc[0].get("itemName", "")

        fila = [
            nombre_producto,
            tid,
            row["date"].strftime("%Y-%m-%d") if not pd.isnull(row["date"]) else "",
            precio,
            cantidad,
            unidad,
            row.get("notes", ""),
            comercio,
            archivo_img,
            fecha_modificacion,
        ]
        cargar_historial.clear()
        df_hist, row_by_key = cargar_historial(worksheet)
        fila_existente = row_by_key.get((nombre_normalizado, tid))

        # decidir si actualiza o inserta (unicidad por (producto_norm, tid))
        if fila_existente:
            if actualizar:  # o actualizar_existente si lo igualaste a 'actualizar'
                worksheet.update(f"A{fila_existente}:J{fila_existente}", [fila])
                st.session_state.flash_ok = "Registro actualizado en HistorialCompras."
                time.sleep(1)
                cargar_historial.clear()
                st.rerun()
            else:
                st.info("Este registro ya existe. Marca 'Actualizar' si deseas modificarlo.")
                st.stop()
        else:
            all_rows = worksheet.get_all_values()
            worksheet.insert_row(fila, index=len(all_rows) + 1)
            st.session_state.flash_ok = "¡Compra registrada!"
            time.sleep(1)
            cargar_historial.clear()
            st.rerun()

    # ===================================================

