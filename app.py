import json
import os
from datetime import timedelta

import streamlit as st
from databricks.sdk import WorkspaceClient


st.set_page_config(
    page_title="Databricks Job Runner Demo",
    layout="wide"
)

st.title("Databricks Job Runner Demo")
st.caption("Envía dos números a un Job de Databricks y muestra el resultado en la app.")


# ============================================================
# Configuración
# ============================================================

TARGET_WORKSPACE_HOST = os.getenv(
    "TARGET_WORKSPACE_HOST",
    "https://adb-830380622316336.16.azuredatabricks.net"
)

TARGET_JOB_ID = os.getenv("TARGET_JOB_ID")

DATABRICKS_CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID")
DATABRICKS_CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")


def get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient(
        host=TARGET_WORKSPACE_HOST,
        client_id=DATABRICKS_CLIENT_ID,
        client_secret=DATABRICKS_CLIENT_SECRET,
    )


def validate_config():
    missing = []

    required_vars = {
        "TARGET_WORKSPACE_HOST": TARGET_WORKSPACE_HOST,
        "TARGET_JOB_ID": TARGET_JOB_ID,
        "DATABRICKS_CLIENT_ID": DATABRICKS_CLIENT_ID,
        "DATABRICKS_CLIENT_SECRET": DATABRICKS_CLIENT_SECRET,
    }

    for name, value in required_vars.items():
        if not value:
            missing.append(name)

    return missing


def run_sum_job(a: float, b: float) -> dict:
    """
    Ejecuta el Job de Databricks pasando parámetros al notebook task.

    Nota:
    - run_now devuelve un waiter.
    - .result() espera a que el run termine.
    - Para leer el output de un notebook task, usamos el task_run_id,
      no necesariamente el parent run_id.
    """

    w = get_workspace_client()

    run = w.jobs.run_now(
        job_id=int(TARGET_JOB_ID),
        notebook_params={
            "a": str(a),
            "b": str(b),
        },
    ).result(timeout=timedelta(minutes=10))

    if not run.tasks:
        raise RuntimeError(
            "El Job terminó, pero no se encontraron tasks en el run. "
            "Valida que el Job tenga al menos un Notebook Task."
        )

    task_run_id = run.tasks[0].run_id

    output = w.jobs.get_run_output(run_id=task_run_id)

    if not output.notebook_output or not output.notebook_output.result:
        raise RuntimeError(
            "El task terminó, pero no devolvió notebook_output.result. "
            "Valida que el notebook use dbutils.notebook.exit(...)."
        )

    raw_result = output.notebook_output.result

    try:
        return json.loads(raw_result)
    except json.JSONDecodeError:
        return {
            "status": "raw_output",
            "raw_result": raw_result,
        }


# ============================================================
# Validación de configuración
# ============================================================

st.subheader("Configuración detectada")

st.write({
    "TARGET_WORKSPACE_HOST": TARGET_WORKSPACE_HOST,
    "TARGET_JOB_ID": TARGET_JOB_ID,
    "DATABRICKS_CLIENT_ID": DATABRICKS_CLIENT_ID,
    "DATABRICKS_CLIENT_SECRET": "***" if DATABRICKS_CLIENT_SECRET else None,
})

missing = validate_config()

if missing:
    st.error(f"Faltan variables requeridas: {', '.join(missing)}")
    st.stop()


# ============================================================
# Formulario de prueba
# ============================================================

st.subheader("Prueba de suma")

with st.form("sum_form"):
    a = st.number_input("Número A", value=10.0)
    b = st.number_input("Número B", value=5.0)

    submitted = st.form_submit_button("Ejecutar Job", type="primary")

if submitted:
    with st.spinner("Ejecutando Job en Databricks..."):
        try:
            result = run_sum_job(a, b)

            if result.get("status") == "success":
                st.success("Job ejecutado correctamente.")

                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric("A", result["a"])

                with col2:
                    st.metric("B", result["b"])

                with col3:
                    st.metric("Resultado", result["result"])

                st.json(result)

            else:
                st.error("El Job devolvió error o salida no esperada.")
                st.json(result)

        except Exception as e:
            st.error("Falló la ejecución del Job.")
            st.exception(e)