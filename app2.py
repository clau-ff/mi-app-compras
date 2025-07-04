import streamlit as st

# 1. LOGIN SIMPLE
st.title("App Compras Familiares")

PASSWORD = "ratas2025"
password_input = st.text_input("Ingresa la clave familiar", type="password")
if password_input != PASSWORD:
    st.warning("Clave incorrecta o pendiente de ingresar.")
    st.stop()

st.success("¡Bienvenida/o!")

# 2. AUTORIZACIÓN GOOGLE DRIVE Y SHEETS (se implementa después)
st.info("En la próxima versión, aquí pediremos autorización Google para acceder a tu Drive y Sheets.")

# 3. SUBSECCIONES (placeholder para las funciones)
opcion = st.selectbox("¿Qué quieres hacer?", [
    "Buscar producto",
    "Ver últimas compras",
    "Registrar compra manual",
    "Análisis de precios",
    "Recomendación de cantidad"
])

if opcion == "Buscar producto":
    st.write("Aquí irá la búsqueda flexible por producto.")
elif opcion == "Ver últimas compras":
    st.write("Aquí mostraremos tus últimas compras y boletas.")
elif opcion == "Registrar compra manual":
    st.write("Aquí podrás ingresar precio y cantidad manualmente.")
elif opcion == "Análisis de precios":
    st.write("Aquí mostraremos si conviene comprar o no.")
elif opcion == "Recomendación de cantidad":
    st.write("Aquí sugeriremos cuántas unidades comprar.")

st.info("Para usar la app, primero debes subir tu base de datos y fotos de boletas a Google Drive, en las carpetas correctas.")

# Notas para desarrollo futuro
st.caption("""
⚙️ Próximos pasos:
- Leer datos de Bluecoins desde Drive (requiere autorización).
- Búsqueda por nombre (fuzzy matching).
- Mostrar imágenes de boletas.
- Registrar datos en Google Sheets.
""")
