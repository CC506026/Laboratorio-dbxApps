import io
import os
import json
import uuid
import hashlib
from datetime import timedelta

import streamlit as st
from databricks.sdk import WorkspaceClient


st.set_page_config(
    page_title="Excel Cross-Workspace Processor",
    layout="wide"
)

st.title("Excel Cross-Workspace Processor")
st.caption(
    "Carga un Excel en la app, lo procesa en otro workspace mediante un Job "
    "y valida que la app no guarde el archivo localmente."
)


# ============================================================
# Configuración
# ============================================================

TARGET_WORKSPACE_HOST = os.getenv("TARGET_WORKSPACE_HOST")
TARGET_JOB_ID = os.getenv("TARGET_JOB_ID")
TARGET_VOLUME_DIR = os.getenv("TARGET_VOLUME_DIR")

DATABRICKS_CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID")
DATABRICKS_CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")


def get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient(
        host=TARGET_WORKSPACE_HOST,
        client_id=DATABRICKS_CLIENT_ID,
        client_secret=DATABRICKS_CLIENT_SECRET,
    )


def validate_config():
    required = {
        "TARGET_WORKSPACE_HOST": TARGET_WORKSPACE_HOST,
        "TARGET_JOB_ID": TARGET_JOB_ID,
        "TARGET_VOLUME_DIR": TARGET_VOLUME_DIR,
        "DATABRICKS_CLIENT_ID": DATABRICKS_CLIENT_ID,
        "DATABRICKS_CLIENT_SECRET": DATABRICKS_CLIENT_SECRET,
    }

    return [name for name, value in required.items() if not value]


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def safe_filename(filename: str) -> str:
    filename = os.path.basename(filename)
    filename = filename.replace(" ", "_")

    allowed = []

    for char in filename:
        if char.isalnum() or char in [".", "_", "-"]:
            allowed.append(char)
        else:
            allowed.append("_")

    return "".join(allowed)


def local_persistence_probe(file_name: str) -> dict:
    """
    Prueba defensiva: busca si el archivo fue escrito accidentalmente
    en ubicaciones locales típicas del contenedor de la app.

    Esto no prueba 'cero persistencia universal', pero sí ayuda a validar
    que nuestro código no escribió el Excel en el filesystem local.
    """

    checked_roots = [
        os.getcwd(),
        "/tmp",
    ]

    matches = []

    for root in checked_roots:
        if not os.path.exists(root):
            continue

        for current_root, _, files in os.walk(root):
            # Evita recorrer demasiado profundo
            depth = current_root.replace(root, "").count(os.sep)
            if depth > 3:
                continue

            if file_name in files:
                matches.append(os.path.join(current_root, file_name))

    return {
        "working_directory": os.getcwd(),
        "checked_roots": checked_roots,
        "local_file_matches": matches,
        "local_file_found": len(matches) > 0,
    }


def upload_to_target_volume(
    w: WorkspaceClient,
    target_path: str,
    content: bytes
) -> None:
    """
    Sube bytes al Unity Catalog Volume del workspace destino.

    Requiere que el service principal de la app tenga:
    - USE CATALOG
    - USE SCHEMA
    - WRITE VOLUME sobre el volume destino
    """

    w.files.upload(
        file_path=target_path,
        contents=io.BytesIO(content),
        overwrite=True,
    )


def run_excel_job(input_path: str, run_id: str, original_file_name: str) -> dict:
    w = get_workspace_client()

    run = w.jobs.run_now(
        job_id=int(TARGET_JOB_ID),
        notebook_params={
            "input_path": input_path,
            "run_id": run_id,
            "original_file_name": original_file_name,
            "delete_input": "true",
        },
    ).result(timeout=timedelta(minutes=15))

    if not run.tasks:
        raise RuntimeError("El Job terminó, pero no se encontraron tasks.")

    task_run_id = run.tasks[0].run_id

    output = w.jobs.get_run_output(run_id=task_run_id)

    if not output.notebook_output or not output.notebook_output.result:
        raise RuntimeError(
            "El Job no devolvió notebook_output.result. "
            "Valida que el notebook use dbutils.notebook.exit(...)."
        )

    return json.loads(output.notebook_output.result)


# ============================================================
# Validación de configuración
# ============================================================

st.subheader("1. Configuración")

missing = validate_config()

config_preview = {
    "TARGET_WORKSPACE_HOST": TARGET_WORKSPACE_HOST,
    "TARGET_JOB_ID": TARGET_JOB_ID,
    "TARGET_VOLUME_DIR": TARGET_VOLUME_DIR,
    "DATABRICKS_CLIENT_ID": DATABRICKS_CLIENT_ID,
    "DATABRICKS_CLIENT_SECRET": "***" if DATABRICKS_CLIENT_SECRET else None,
}

st.json(config_preview)

if missing:
    st.error(f"Faltan variables requeridas: {', '.join(missing)}")
    st.stop()


# ============================================================
# Carga de Excel
# ============================================================

st.subheader("2. Carga de Excel")

uploaded_file = st.file_uploader(
    "Carga un archivo Excel .xlsx",
    type=["xlsx"]
)

if uploaded_file is None:
    st.info("Carga un Excel para iniciar.")
    st.stop()


file_bytes = uploaded_file.getvalue()
file_name = safe_filename(uploaded_file.name)
file_size = len(file_bytes)
file_hash = sha256_bytes(file_bytes)

st.write({
    "file_name": file_name,
    "file_size_bytes": file_size,
    "sha256": file_hash,
})


# ============================================================
# Ejecución
# ============================================================

st.subheader("3. Procesamiento cross-workspace")

if st.button("Procesar Excel en workspace destino", type="primary"):

    run_id = str(uuid.uuid4())
    target_input_path = f"{TARGET_VOLUME_DIR}/input/{run_id}_{file_name}"

    local_probe_before = local_persistence_probe(file_name)

    with st.spinner("Subiendo archivo al Volume del workspace destino..."):
        w = get_workspace_client()
        upload_to_target_volume(
            w=w,
            target_path=target_input_path,
            content=file_bytes,
        )

    with st.spinner("Ejecutando Job en workspace destino..."):
        result = run_excel_job(
            input_path=target_input_path,
            run_id=run_id,
            original_file_name=file_name,
        )

    local_probe_after = local_persistence_probe(file_name)

    st.success("Proceso terminado.")

    st.markdown("### Resultado del Job")
    st.json(result)

    st.markdown("### Evidencia de no persistencia local en la app")
    st.json({
        "explanation": (
            "La app recibió el archivo en memoria, lo subió al Volume del workspace destino "
            "y no creó un archivo local con el nombre del Excel en las rutas inspeccionadas."
        ),
        "local_probe_before": local_probe_before,
        "local_probe_after": local_probe_after,
        "app_workspace_written_file": local_probe_after["local_file_found"],
    })

    if result.get("status") == "success":
        processing = result.get("processing_result", {})

        st.markdown("### Métricas del procesamiento")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Filas originales", processing.get("original_rows"))

        with col2:
            st.metric("Filas finales", processing.get("final_rows"))

        with col3:
            st.metric("Duplicados originales", processing.get("original_duplicates"))

        with col4:
            st.metric("Duplicados finales", processing.get("final_duplicates"))

        st.markdown("### Vista previa transformada")
        st.dataframe(processing.get("preview", []), use_container_width=True)

        st.markdown("### Perfil de columnas")
        st.dataframe(processing.get("profile", []), use_container_width=True)