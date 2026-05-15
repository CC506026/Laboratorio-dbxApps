import io
import re
from datetime import datetime

import pandas as pd
import streamlit as st


# ============================================================
# Configuración general de la app
# ============================================================

st.set_page_config(
    page_title="Excel Transformer Demo",
    page_icon="📊",
    layout="wide"
)

st.title("Excel Transformer Demo")
st.caption("Carga un Excel dummy, aplica transformaciones básicas y descarga el resultado.")


# ============================================================
# Funciones auxiliares
# ============================================================

def normalize_column_name(col: str) -> str:
    """
    Normaliza nombres de columnas:
    - quita espacios extremos
    - convierte a minúsculas
    - reemplaza espacios y caracteres raros por _
    - elimina guiones bajos duplicados
    """
    col = str(col).strip().lower()
    col = re.sub(r"[^\w]+", "_", col)
    col = re.sub(r"_+", "_", col)
    col = col.strip("_")
    return col


def read_excel_file(uploaded_file) -> pd.DataFrame:
    """
    Lee el archivo Excel cargado desde Streamlit.
    Por ahora toma la primera hoja.
    """
    return pd.read_excel(uploaded_file, engine="openpyxl")


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformaciones dummy/base.

    Estas transformaciones son intencionalmente simples:
    1. Normaliza nombres de columnas.
    2. Elimina filas completamente vacías.
    3. Limpia espacios en columnas texto.
    4. Convierte strings vacíos a NA.
    5. Elimina duplicados exactos.
    6. Agrega columnas técnicas de auditoría.
    """

    df = df.copy()

    # 1. Normalizar nombres de columnas
    df.columns = [normalize_column_name(col) for col in df.columns]

    # 2. Eliminar filas completamente vacías
    df = df.dropna(how="all")

    # 3. Limpiar columnas de texto
    text_columns = df.select_dtypes(include=["object"]).columns

    for col in text_columns:
        df[col] = (
            df[col]
            .astype("string")
            .str.strip()
        )

    # 4. Convertir strings vacíos a valores nulos
    df = df.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

    # 5. Eliminar duplicados exactos
    df = df.drop_duplicates()

    # 6. Agregar columnas técnicas
    df["_processed_at"] = datetime.now()
    df["_source"] = "streamlit_excel_upload"

    return df


def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    """
    Convierte un DataFrame a archivo Excel en memoria.
    Esto permite descargar el resultado desde Streamlit.
    """
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="transformed_data")

    return output.getvalue()


def create_dummy_excel() -> bytes:
    """
    Crea un Excel dummy para pruebas.
    Útil para validar la app sin depender de archivos externos.
    """
    dummy_df = pd.DataFrame({
        " ID Cliente ": [1, 2, 2, 3, None],
        "Nombre Cliente": ["  Ana ", "Luis", "Luis", " Carlos ", None],
        " Monto Venta ": [1000, 2500, 2500, 1800, None],
        "Fecha Venta": ["2026-01-10", "2026-01-11", "2026-01-11", "2026-01-12", None],
        "Estatus": [" Pagado ", "Pendiente", "Pendiente", "", None]
    })

    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        dummy_df.to_excel(writer, index=False, sheet_name="dummy_data")

    return output.getvalue()


# ============================================================
# Sidebar
# ============================================================

st.sidebar.header("Opciones")

show_profile = st.sidebar.checkbox(
    "Mostrar perfil del dataset",
    value=True
)

show_nulls = st.sidebar.checkbox(
    "Mostrar conteo de nulos",
    value=True
)

show_columns = st.sidebar.checkbox(
    "Mostrar columnas normalizadas",
    value=True
)


# ============================================================
# Descarga de Excel dummy
# ============================================================

st.subheader("1. Descargar Excel dummy")

dummy_excel = create_dummy_excel()

st.download_button(
    label="Descargar Excel dummy",
    data=dummy_excel,
    file_name="dummy_input.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)


# ============================================================
# Carga de archivo
# ============================================================

st.subheader("2. Cargar Excel")

uploaded_file = st.file_uploader(
    "Carga un archivo Excel .xlsx",
    type=["xlsx"]
)

if uploaded_file is None:
    st.info("Carga un Excel para iniciar el proceso.")
    st.stop()


# ============================================================
# Lectura del archivo
# ============================================================

try:
    raw_df = read_excel_file(uploaded_file)
except Exception as e:
    st.error("No se pudo leer el archivo Excel.")
    st.exception(e)
    st.stop()


st.success("Archivo cargado correctamente.")


# ============================================================
# Vista previa raw
# ============================================================

st.subheader("3. Vista previa del archivo original")

st.dataframe(raw_df, use_container_width=True)

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Filas originales", raw_df.shape[0])

with col2:
    st.metric("Columnas originales", raw_df.shape[1])

with col3:
    st.metric("Duplicados exactos", raw_df.duplicated().sum())


# ============================================================
# Transformación
# ============================================================

st.subheader("4. Transformaciones")

if st.button("Ejecutar transformaciones", type="primary"):

    transformed_df = transform_dataframe(raw_df)

    st.session_state["transformed_df"] = transformed_df

    st.success("Transformaciones ejecutadas correctamente.")


# ============================================================
# Resultado
# ============================================================

if "transformed_df" in st.session_state:

    transformed_df = st.session_state["transformed_df"]

    st.subheader("5. Resultado transformado")

    st.dataframe(transformed_df, use_container_width=True)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Filas transformadas", transformed_df.shape[0])

    with col2:
        st.metric("Columnas transformadas", transformed_df.shape[1])

    with col3:
        st.metric("Duplicados finales", transformed_df.duplicated().sum())

    if show_columns:
        st.markdown("### Columnas finales")
        st.write(list(transformed_df.columns))

    if show_nulls:
        st.markdown("### Nulos por columna")
        nulls_df = (
            transformed_df
            .isna()
            .sum()
            .reset_index()
            .rename(columns={"index": "columna", 0: "nulos"})
        )

        st.dataframe(nulls_df, use_container_width=True)

    if show_profile:
        st.markdown("### Perfil básico")
        profile_df = pd.DataFrame({
            "columna": transformed_df.columns,
            "tipo_dato": transformed_df.dtypes.astype(str).values,
            "nulos": transformed_df.isna().sum().values,
            "valores_unicos": transformed_df.nunique(dropna=True).values
        })

        st.dataframe(profile_df, use_container_width=True)

    # Descargar resultado
    transformed_excel = dataframe_to_excel_bytes(transformed_df)

    st.download_button(
        label="Descargar Excel transformado",
        data=transformed_excel,
        file_name="transformed_output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )