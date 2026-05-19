import ctypes
import gc
import json
import os
import time
import uuid
from datetime import timedelta
from typing import Any, Dict, Optional

import streamlit as st
from databricks.sdk import WorkspaceClient


# ============================================================
# Configuración de la app
# ============================================================

st.set_page_config(
    page_title="Excel Processor",
    layout="centered"
)

st.title("Procesador de Excel")
st.caption(
    "La app no abre ni procesa el Excel. Solo lo mantiene en memoria, "
    "lo envía al workspace destino y ejecuta un Job remoto."
)


# ============================================================
# Variables de entorno
# ============================================================

TARGET_WORKSPACE_HOST = os.getenv("TARGET_WORKSPACE_HOST")
TARGET_JOB_ID = os.getenv("TARGET_JOB_ID")
TARGET_TASK_KEY = os.getenv("TARGET_TASK_KEY", "Procesamiento_de_exceles")
TARGET_VOLUME_DIR = os.getenv("TARGET_VOLUME_DIR")

DATABRICKS_CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID")
DATABRICKS_CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")


# ============================================================
# Cliente Databricks para Workspace B
# ============================================================

def get_workspace_client() -> WorkspaceClient:
    """
    Crea cliente contra el workspace destino.

    La app usa su service principal.
    Ese service principal debe tener permisos en Workspace B:
    - WRITE VOLUME sobre el volume destino.
    - READ VOLUME sobre el output si se va a descargar.
    - CAN MANAGE RUN sobre el Job.
    """
    return WorkspaceClient(
        host=TARGET_WORKSPACE_HOST,
        client_id=DATABRICKS_CLIENT_ID,
        client_secret=DATABRICKS_CLIENT_SECRET,
    )


def validate_config() -> list[str]:
    required = {
        "TARGET_WORKSPACE_HOST": TARGET_WORKSPACE_HOST,
        "TARGET_JOB_ID": TARGET_JOB_ID,
        "TARGET_TASK_KEY": TARGET_TASK_KEY,
        "TARGET_VOLUME_DIR": TARGET_VOLUME_DIR,
        "DATABRICKS_CLIENT_ID": DATABRICKS_CLIENT_ID,
        "DATABRICKS_CLIENT_SECRET": DATABRICKS_CLIENT_SECRET,
    }

    return [name for name, value in required.items() if not value]


# ============================================================
# Utilidades de memoria
# ============================================================

def get_memory_info(buffer_view: memoryview) -> Dict[str, Any]:
    """
    Devuelve información del buffer en memoria.

    Nota:
    - id(...) es el identificador del objeto Python.
    - buffer_virtual_address_hex intenta mostrar la dirección virtual del buffer.
    - Esto es diagnóstico, no una garantía de control total sobre RAM.
    """

    info = {
        "memoryview_object_id_hex": hex(id(buffer_view)),
        "buffer_size_bytes": buffer_view.nbytes,
        "buffer_readonly": buffer_view.readonly,
        "buffer_virtual_address_hex": None,
        "note": None,
    }

    try:
        if buffer_view.readonly:
            info["note"] = (
                "El buffer es read-only. Se puede leer, pero no sobrescribir directamente."
            )
        elif buffer_view.nbytes == 0:
            info["note"] = "El buffer está vacío."
        else:
            address = ctypes.addressof(ctypes.c_char.from_buffer(buffer_view))
            info["buffer_virtual_address_hex"] = hex(address)
            info["note"] = (
                "Dirección virtual del buffer dentro del proceso Python."
            )

    except Exception as e:
        info["note"] = f"No se pudo obtener dirección virtual: {type(e).__name__}: {str(e)}"

    return info


def zeroize_memoryview(buffer_view: memoryview) -> Dict[str, Any]:
    """
    Sobrescribe el buffer con ceros como best effort.

    Limitación:
    Python/Streamlit podrían tener copias internas que este código no controla.
    """

    result = {
        "zeroize_attempted": True,
        "zeroize_success": False,
        "bytes_zeroized": 0,
        "error": None,
    }

    try:
        if buffer_view.readonly:
            result["error"] = "El buffer es read-only; no se pudo sobrescribir."
            return result

        total = buffer_view.nbytes

        if total == 0:
            result["zeroize_success"] = True
            return result

        chunk_size = min(1024 * 1024, total)
        zero_chunk = b"\x00" * chunk_size

        for start in range(0, total, chunk_size):
            end = min(start + chunk_size, total)
            buffer_view[start:end] = zero_chunk[: end - start]

        result["zeroize_success"] = True
        result["bytes_zeroized"] = total

    except Exception as e:
        result["error"] = f"{type(e).__name__}: {str(e)}"

    return result


def clear_download_buffer() -> None:
    """
    Limpia el buffer de descarga guardado temporalmente en session_state.

    Esta función se ejecuta cuando el usuario da click en el botón de descarga.
    No puede controlar copias internas de Streamlit o del navegador.
    """
    output_buffer = st.session_state.get("output_buffer")

    if isinstance(output_buffer, bytearray):
        for i in range(len(output_buffer)):
            output_buffer[i] = 0

    st.session_state.pop("output_buffer", None)
    st.session_state.pop("output_file_name", None)
    st.session_state.pop("output_mime", None)
    st.session_state.pop("output_memory_info", None)

    gc.collect()


# ============================================================
# Utilidades de archivo
# ============================================================

def safe_filename(filename: str) -> str:
    """
    Limpia el nombre del archivo para usarlo en una ruta.
    """
    filename = os.path.basename(filename).replace(" ", "_")

    clean = []

    for char in filename:
        if char.isalnum() or char in [".", "_", "-"]:
            clean.append(char)
        else:
            clean.append("_")

    return "".join(clean)


def infer_mime_type(file_path: str) -> str:
    lower = file_path.lower()

    if lower.endswith(".xlsx"):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    if lower.endswith(".csv"):
        return "text/csv"

    if lower.endswith(".json"):
        return "application/json"

    return "application/octet-stream"


# ============================================================
# Operaciones contra Workspace B
# ============================================================

def upload_input_to_workspace_b(
    w: WorkspaceClient,
    uploaded_file,
    target_path: str,
) -> None:
    """
    Sube el archivo recibido al Volume del Workspace B.

    No escribe en disco local.
    No usa pandas.
    No abre el Excel.
    """
    uploaded_file.seek(0)

    w.files.upload(
        file_path=target_path,
        contents=uploaded_file,
        overwrite=True,
    )


def run_remote_job(
    w: WorkspaceClient,
    input_path: str,
    run_id: str,
    original_file_name: str,
) -> Dict[str, Any]:
    """
    Ejecuta el Job remoto.

    El Job debe devolver con dbutils.notebook.exit(...) un JSON con al menos:
    - status
    - output_path, si se quiere descarga
    """

    waiter = w.jobs.run_now(
        job_id=int(TARGET_JOB_ID),
        notebook_params={
            "input_path": input_path,
            "run_id": run_id,
            "original_file_name": original_file_name,
            "delete_input": "false",
        },
    )

    run = waiter.result(timeout=timedelta(minutes=30))

    if not run.tasks:
        raise RuntimeError("El Job terminó, pero no se encontraron tasks.")

    selected_task = None

    for task in run.tasks:
        if task.task_key == TARGET_TASK_KEY:
            selected_task = task
            break

    if selected_task is None:
        selected_task = run.tasks[0]

    output = w.jobs.get_run_output(run_id=selected_task.run_id)

    if not output.notebook_output or not output.notebook_output.result:
        raise RuntimeError(
            "El Job no devolvió notebook_output.result. "
            "Valida que el notebook use dbutils.notebook.exit(...)."
        )

    raw_result = output.notebook_output.result.strip()

    return json.loads(raw_result)


def load_output_to_memory(
    w: WorkspaceClient,
    output_path: str,
) -> bytearray:
    """
    Descarga el output desde Workspace B hacia memoria de la app.

    No escribe en disco local.
    Solo se mantiene en memoria para habilitar el botón de descarga.
    """

    response = w.files.download(file_path=output_path)

    try:
        content = response.contents.read()
        return bytearray(content)
    finally:
        try:
            response.contents.close()
        except Exception:
            pass


# ============================================================
# Validación inicial
# ============================================================

missing = validate_config()

if missing:
    st.error(f"Faltan variables de configuración: {', '.join(missing)}")
    st.stop()


with st.expander("Configuración técnica"):
    st.json({
        "TARGET_WORKSPACE_HOST": TARGET_WORKSPACE_HOST,
        "TARGET_JOB_ID": TARGET_JOB_ID,
        "TARGET_TASK_KEY": TARGET_TASK_KEY,
        "TARGET_VOLUME_DIR": TARGET_VOLUME_DIR,
        "DATABRICKS_CLIENT_ID": DATABRICKS_CLIENT_ID,
        "DATABRICKS_CLIENT_SECRET": "***" if DATABRICKS_CLIENT_SECRET else None,
    })


# ============================================================
# Carga del Excel
# ============================================================

uploaded_file = st.file_uploader(
    "Carga un archivo Excel",
    type=["xlsx"],
)

if uploaded_file is None:
    st.info("Carga un archivo para iniciar.")
    st.stop()


file_name = safe_filename(uploaded_file.name)
buffer_view = uploaded_file.getbuffer()

st.subheader("Archivo recibido en memoria")

memory_info_before = get_memory_info(buffer_view)

st.json({
    "file_name": file_name,
    "memory_info": memory_info_before,
})

st.caption(
    "La app todavía no abrió ni procesó el Excel. Solo inspeccionó el buffer en memoria."
)


# ============================================================
# Procesamiento remoto
# ============================================================

if st.button("Enviar a procesar", type="primary"):

    run_id = str(uuid.uuid4())
    target_input_path = f"{TARGET_VOLUME_DIR}/input/{run_id}_{file_name}"

    w = get_workspace_client()

    timings = {}

    try:
        # --------------------------------------------------------
        # 1. Subir archivo al Workspace B
        # --------------------------------------------------------
        t0 = time.perf_counter()

        with st.spinner("Enviando archivo al workspace destino..."):
            upload_input_to_workspace_b(
                w=w,
                uploaded_file=uploaded_file,
                target_path=target_input_path,
            )

        timings["upload_seconds"] = round(time.perf_counter() - t0, 3)

        st.success("Archivo enviado al workspace destino.")

        # --------------------------------------------------------
        # 2. Borrar buffer de entrada en memoria de la app
        # --------------------------------------------------------
        zeroize_result = zeroize_memoryview(buffer_view)

        try:
            buffer_view.release()
        except Exception:
            pass

        gc.collect()

        st.subheader("Limpieza del buffer de entrada")
        st.json(zeroize_result)

        # --------------------------------------------------------
        # 3. Ejecutar Job remoto
        # --------------------------------------------------------
        t1 = time.perf_counter()

        with st.spinner("Procesando en el workspace destino..."):
            job_result = run_remote_job(
                w=w,
                input_path=target_input_path,
                run_id=run_id,
                original_file_name=file_name,
            )

        timings["job_wait_seconds"] = round(time.perf_counter() - t1, 3)

        st.session_state["last_job_result"] = job_result

        # Guardamos solo metadata, no archivo.
        st.session_state["last_output_path"] = job_result.get("output_path")
        st.session_state["last_status"] = job_result.get("status")

        st.success("Archivo procesado correctamente en el workspace destino.")

        st.caption(
            "La app no abrió ni mostró el contenido del Excel. "
            "El resultado quedó persistido en el workspace donde corre el Job."
        )

        with st.expander("Resultado técnico del Job"):
            st.json(job_result)

        with st.expander("Tiempos"):
            st.json(timings)

    except Exception as e:
        st.error("Falló el procesamiento.")
        st.exception(e)

    finally:
        # Eliminamos referencias locales al input.
        try:
            del uploaded_file
        except Exception:
            pass

        try:
            del buffer_view
        except Exception:
            pass

        gc.collect()


# ============================================================
# Descarga del resultado
# ============================================================

output_path = st.session_state.get("last_output_path")
last_status = st.session_state.get("last_status")

if last_status == "success" and output_path:

    st.divider()
    st.subheader("Descarga del resultado")

    st.write("El archivo procesado está listo para descarga.")
    st.code(output_path)

    # Este botón carga el output en memoria de la app.
    # No se descarga automáticamente todavía.
    if st.button("Preparar descarga"):
        w = get_workspace_client()

        with st.spinner("Cargando output en memoria para descarga..."):
            output_buffer = load_output_to_memory(
                w=w,
                output_path=output_path,
            )

        output_view = memoryview(output_buffer)

        st.session_state["output_buffer"] = output_buffer
        st.session_state["output_file_name"] = os.path.basename(output_path)
        st.session_state["output_mime"] = infer_mime_type(output_path)
        st.session_state["output_memory_info"] = get_memory_info(output_view)

        try:
            output_view.release()
        except Exception:
            pass

        gc.collect()

    if "output_buffer" in st.session_state:

        st.subheader("Output cargado temporalmente en memoria")

        st.json(st.session_state.get("output_memory_info"))

        st.caption(
            "El output está en memoria solo para poder construir la descarga. "
            "Después del click de descarga se intentará limpiar el buffer temporal."
        )

        st.download_button(
            label="Descargar archivo procesado",
            data=bytes(st.session_state["output_buffer"]),
            file_name=st.session_state["output_file_name"],
            mime=st.session_state["output_mime"],
            on_click=clear_download_buffer,
        )

elif last_status == "success" and not output_path:
    st.warning(
        "El Job terminó correctamente, pero no devolvió output_path. "
        "No hay archivo para descargar desde la app."
    )