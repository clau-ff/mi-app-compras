# Versión 3 de la App Compras Familiares (app7.py)
# Mejora: corrige orientación de imagen boleta, cálculo valor unitario promedio y activación del análisis

# [...Código completo mantenido previamente...]

        archivo_img = df_pic[df_pic['transactionID'] == tid]['pictureFileName'].values
        if len(archivo_img) > 0:
            ruta = descargar_y_mostrar_imagen(archivo_img[0])
            if ruta:
                ext = os.path.splitext(ruta)[-1].lower()
                if ext in [".jpg", ".jpeg", ".png"]:
                    imagen = Image.open(ruta)
                    if imagen.width > imagen.height:
                        imagen = imagen.rotate(90, expand=True)
                    st.image(imagen, caption="Boleta")
                elif ext == ".pdf":
                    with open(ruta, "rb") as f:
                        base64_pdf = base64.b64encode(f.read()).decode('utf-8')
                    st.components.v1.html(
                        f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="700" height="900" type="application/pdf"></iframe>',
                        height=920
                    )

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

    # --- Análisis extendido ---
    registros_sheet = set(worksheet.col_values(2))
    if trans_ids_mostradas.issubset(registros_sheet):
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
            st.info("No hay datos suficientes para análisis.")
else:
    st.info("Ingresa un producto para comenzar.")