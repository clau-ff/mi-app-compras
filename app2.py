import streamlit as st

# 1. LOGIN SIMPLE
st.title("App Compras Familiares")

PASSWORD = "ratas2025"
password_input = st.text_input("Ingresa la clave familiar", type="password")
if password_input != PASSWORD:
    st.warning("Clave incorrecta o pendiente de ingresar.")
    st.stop()

st.success("¡Bienvenida/o!")

# 2. AUTORIZACIÓN GOOGLE DRIVE Y SHEETS (próximo paso)
st.info("Haz clic para autorizar acceso temporal a Google Drive y Sheets. Esto es seguro y privado para tu familia.")

# Aquí se implementará el flujo de autorización con pydrive2/gspread

# 3. LEER ARCHIVO BLUECOINS MÁS RECIENTE (sección a completar)
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
