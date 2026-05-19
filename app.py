import ctypes
import gc
import io
import json
import os
import time
import uuid
from datetime import timedelta
from typing import Any, Dict, Tuple

import streamlit as st
from databricks.sdk import WorkspaceClient


# ============================================================
# Configuración general
# ============================================================

st.set_page_config(
    page_title="Excel Processor",
    layout="centered"
)

st.title("Procesador de Excel")
st.caption(
    "La app no abre ni procesa el Excel. Lo recibe en memoria, "
    "lo envía al workspace destino, ejecuta un Job remoto y permite descargar el resultado."
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
# Estado de Streamlit
# ============================================================

def init_state() -> None:
    """
    Inicializa variables de estado.

    Importante:
    - No guardamos el Excel de entrada en session_state.
    - Solo guardamos metadata, latencias y, temporalmente, el output para descarga.
    """

    defaults = {
        "uploader_version": 0,
        "last_status": None,
        "last_job_result": None,
        "last_output_path": None,
        "last_latency": None,
        "last_input_memory_info": None,
        "last_input_zeroize_result": None,
        "last_target_input_path": None,
        "output_buffer": None,
        "output_file_name": None,
        "output_mime": None,
        "output_memory_info": None,
        "output_download_latency": None,
        "download_cleanup_result": None,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


init_state()


# ============================================================
# Cliente Databricks
# ============================================================

def get_workspace_client() -> WorkspaceClient:
    """
    Crea cliente contra Workspace B.

    Este cliente usa el service principal de la app.
    El service principal necesita:
    - WRITE VOLUME en el Volume destino.
    - READ VOLUME en el output.
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
    Obtiene información diagnóstica del buffer en memoria.

    La dirección virtual sirve como evidencia de que estamos manipulando
    un buffer en memoria del proceso Python, no una ruta física.

    Limitación:
    Esto no prueba ausencia total de copias internas de Streamlit/Python.
    """

    info = {
        "memoryview_object_id_hex": hex(id(buffer_view)),
        "buffer_size_bytes": buffer_view.nbytes,
        "buffer_readonly": buffer_view.readonly,
        "buffer_virtual_address_hex": None,
        "note": None,
    }

    try:
        if buffer_view.nbytes == 0:
            info["note"] = "El buffer está vacío."

        elif buffer_view.readonly:
            info["note"] = (
                "El buffer es read-only. Se puede inspeccionar, pero no sobrescribir directamente."
            )

        else:
            address = ctypes.addressof(ctypes.c_char.from_buffer(buffer_view))
            info["buffer_virtual_address_hex"] = hex(address)
            info["note"] = "Dirección virtual del buffer dentro del proceso Python."

    except Exception as e:
        info["note"] = f"No se pudo obtener dirección virtual: {type(e).__name__}: {str(e)}"

    return info


def zeroize_memoryview(buffer_view: memoryview) -> Dict[str, Any]:
    """
    Sobrescribe el buffer con ceros como best effort.

    Esto limpia el buffer que controlamos directamente.
    No garantiza borrado absoluto de todas las copias internas del runtime.
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
    Limpia el buffer temporal de descarga cuando el usuario da click en descargar.

    Limpia:
    - buffer del archivo descargado desde Workspace B
    - metadata temporal de descarga

    Limitación:
    Streamlit puede crear copias internas para servir la descarga.
    Este código limpia el buffer que controlamos en session_state.
    """

    cleanup_result = {
        "cleanup_attempted": True,
        "zeroize_success": False,
        "bytes_zeroized": 0,
        "error": None,
    }

    output_buffer = st.session_state.get("output_buffer")

    try:
        if isinstance(output_buffer, io.BytesIO):
            buffer_view = output_buffer.getbuffer()

            cleanup_result["memory_info_before_cleanup"] = get_memory_info(buffer_view)

            zeroize_result = zeroize_memoryview(buffer_view)
            cleanup_result.update(zeroize_result)

            buffer_view.release()
            output_buffer.close()

        else:
            cleanup_result["error"] = "No había buffer de descarga tipo BytesIO."

    except Exception as e:
        cleanup_result["error"] = f"{type(e).__name__}: {str(e)}"

    st.session_state["output_buffer"] = None
    st.session_state["output_file_name"] = None
    st.session_state["output_mime"] = None
    st.session_state["output_memory_info"] = None
    st.session_state["output_download_latency"] = None
    st.session_state["download_cleanup_result"] = cleanup_result

    gc.collect()


# ============================================================
# Utilidades de archivo
# ============================================================

def safe_filename(filename: str) -> str:
    """
    Limpia el nombre del archivo para construir rutas seguras en el Volume.
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


def mb_per_second(size_bytes: int, seconds: float) -> float | None:
    """
    Calcula throughput aproximado en MB/s.
    """

    if seconds <= 0:
        return None

    return round((size_bytes / 1024 / 1024) / seconds, 4)


# ============================================================
# Operaciones Workspace A → Workspace B
# ============================================================

def upload_input_to_workspace_b(
    w: WorkspaceClient,
    uploaded_file,
    target_path: str,
) -> Dict[str, Any]:
    """
    Sube el Excel desde memoria de Workspace A al Volume de Workspace B.

    No escribe archivo local.
    No abre el Excel.
    No usa pandas.
    """

    uploaded_file.seek(0)

    t0 = time.perf_counter()

    w.files.upload(
        file_path=target_path,
        contents=uploaded_file,
        overwrite=True,
    )

    elapsed = round(time.perf_counter() - t0, 3)

    return {
        "operation": "upload_input_a_to_b",
        "target_path": target_path,
        "seconds": elapsed,
    }


def run_remote_job(
    w: WorkspaceClient,
    input_path: str,
    run_id: str,
    original_file_name: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Ejecuta el Job remoto y mide:

    - job_submit_api_seconds:
      tiempo que tarda la app en mandar el run_now al Workspace B.

    - job_wait_until_finished_seconds:
      tiempo esperando a que Databricks termine el Job.
      Incluye cola, cold start, setup, ejecución real y cleanup.

    - job_output_fetch_seconds:
      tiempo para traer el output JSON del task.
    """

    latency = {}

    # ------------------------------------------------------------
    # 1. Mandar run_now
    # ------------------------------------------------------------
    t_submit = time.perf_counter()

    waiter = w.jobs.run_now(
        job_id=int(TARGET_JOB_ID),
        notebook_params={
            "input_path": input_path,
            "run_id": run_id,
            "original_file_name": original_file_name,
            "delete_input": "false",
        },
    )

    latency["job_submit_api_seconds"] = round(time.perf_counter() - t_submit, 3)

    # ------------------------------------------------------------
    # 2. Esperar fin del Job
    # ------------------------------------------------------------
    t_wait = time.perf_counter()

    run = waiter.result(timeout=timedelta(minutes=30))

    latency["job_wait_until_finished_seconds"] = round(time.perf_counter() - t_wait, 3)
    latency["parent_run_id"] = run.run_id

    if not run.tasks:
        raise RuntimeError("El Job terminó, pero no se encontraron tasks.")

    selected_task = None

    for task in run.tasks:
        if task.task_key == TARGET_TASK_KEY:
            selected_task = task
            break

    if selected_task is None:
        selected_task = run.tasks[0]

    latency["selected_task_key"] = selected_task.task_key
    latency["task_run_id"] = selected_task.run_id

    # ------------------------------------------------------------
    # 3. Recuperar output JSON del task
    # ------------------------------------------------------------
    t_output = time.perf_counter()

    output = w.jobs.get_run_output(run_id=selected_task.run_id)

    latency["job_output_fetch_seconds"] = round(time.perf_counter() - t_output, 3)

    if not output.notebook_output or not output.notebook_output.result:
        raise RuntimeError(
            "El Job no devolvió notebook_output.result. "
            "Valida que el notebook use dbutils.notebook.exit(...)."
        )

    raw_result = output.notebook_output.result.strip()

    job_result = json.loads(raw_result)

    return job_result, latency


# ============================================================
# Operaciones Workspace B → Workspace A
# ============================================================

def load_output_from_workspace_b(
    w: WorkspaceClient,
    output_path: str,
) -> Tuple[io.BytesIO, Dict[str, Any]]:
    """
    Descarga el archivo procesado desde Workspace B hacia memoria de Workspace A.

    No escribe archivo local.
    Se usa BytesIO porque Streamlit puede entregarlo al navegador.
    """

    latency = {
        "operation": "download_output_b_to_a",
        "output_path": output_path,
        "download_api_open_seconds": None,
        "download_stream_read_seconds": None,
        "download_total_seconds": None,
        "downloaded_bytes": 0,
        "download_throughput_mb_s": None,
    }

    t_total = time.perf_counter()

    # Abrir stream de descarga
    t_open = time.perf_counter()
    response = w.files.download(file_path=output_path)
    latency["download_api_open_seconds"] = round(time.perf_counter() - t_open, 3)

    buffer = io.BytesIO()

    try:
        # Leer contenido por chunks para evitar una copia monolítica innecesaria.
        t_read = time.perf_counter()

        while True:
            chunk = response.contents.read(1024 * 1024)

            if not chunk:
                break

            buffer.write(chunk)
            latency["downloaded_bytes"] += len(chunk)

        latency["download_stream_read_seconds"] = round(time.perf_counter() - t_read, 3)

    finally:
        try:
            response.contents.close()
        except Exception:
            pass

    latency["download_total_seconds"] = round(time.perf_counter() - t_total, 3)

    latency["download_throughput_mb_s"] = mb_per_second(
        size_bytes=latency["downloaded_bytes"],
        seconds=latency["download_total_seconds"],
    )

    buffer.seek(0)

    return buffer, latency


# ============================================================
# UI: Configuración
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
# UI: Carga del Excel
# ============================================================

uploaded_file = st.file_uploader(
    "Carga un archivo Excel",
    type=["xlsx"],
    key=f"excel_uploader_{st.session_state['uploader_version']}",
)

if uploaded_file is not None:

    file_name = safe_filename(uploaded_file.name)
    buffer_view = uploaded_file.getbuffer()
    memory_info = get_memory_info(buffer_view)

    st.subheader("Archivo recibido en memoria")

    st.json({
        "file_name": file_name,
        "memory_info": memory_info,
    })

    st.caption(
        "La app todavía no abrió ni procesó el Excel. "
        "Solo inspeccionó el buffer en memoria."
    )

    if st.button("Enviar a procesar", type="primary"):

        run_id = str(uuid.uuid4())
        target_input_path = f"{TARGET_VOLUME_DIR}/input/{run_id}_{file_name}"

        full_latency = {
            "run_id": run_id,
            "target_input_path": target_input_path,
            "input_file_name": file_name,
            "input_size_bytes": buffer_view.nbytes,
        }

        t_flow = time.perf_counter()

        try:
            w = get_workspace_client()

            # --------------------------------------------------------
            # 1. Subir Excel A → B
            # --------------------------------------------------------
            with st.spinner("Enviando archivo al workspace destino..."):
                upload_latency = upload_input_to_workspace_b(
                    w=w,
                    uploaded_file=uploaded_file,
                    target_path=target_input_path,
                )

            upload_latency["uploaded_bytes"] = buffer_view.nbytes
            upload_latency["upload_throughput_mb_s"] = mb_per_second(
                size_bytes=buffer_view.nbytes,
                seconds=upload_latency["seconds"],
            )

            full_latency["upload_a_to_b"] = upload_latency

            # --------------------------------------------------------
            # 2. Limpiar buffer de entrada en Workspace A
            # --------------------------------------------------------
            zeroize_result = zeroize_memoryview(buffer_view)

            try:
                buffer_view.release()
            except Exception:
                pass

            gc.collect()

            # --------------------------------------------------------
            # 3. Ejecutar Job remoto
            # --------------------------------------------------------
            with st.spinner("Procesando en el workspace destino..."):
                job_result, job_latency = run_remote_job(
                    w=w,
                    input_path=target_input_path,
                    run_id=run_id,
                    original_file_name=file_name,
                )

            full_latency["job_control_and_execution"] = job_latency
            full_latency["total_until_job_result_seconds"] = round(
                time.perf_counter() - t_flow,
                3,
            )

            # --------------------------------------------------------
            # 4. Guardar solo metadata en session_state
            # --------------------------------------------------------
            st.session_state["last_status"] = job_result.get("status")
            st.session_state["last_job_result"] = job_result
            st.session_state["last_output_path"] = job_result.get("output_path")
            st.session_state["last_latency"] = full_latency
            st.session_state["last_input_memory_info"] = memory_info
            st.session_state["last_input_zeroize_result"] = zeroize_result
            st.session_state["last_target_input_path"] = target_input_path

            # Resetea el file_uploader para reducir retención del input en el widget.
            st.session_state["uploader_version"] += 1

            st.rerun()

        except Exception as e:
            st.error("Falló el procesamiento.")
            st.exception(e)


# ============================================================
# UI: Resultado del procesamiento
# ============================================================

if st.session_state["last_job_result"] is not None:

    st.divider()
    st.subheader("Última ejecución")

    status = st.session_state["last_status"]
    job_result = st.session_state["last_job_result"]

    if status == "success":
        st.success("Archivo procesado correctamente en el workspace destino.")
    elif status == "error":
        st.error("El Job terminó, pero el procesamiento interno falló.")
    else:
        st.warning(f"Estado no reconocido: {status}")

    st.caption(
        "La app no abrió ni mostró el contenido del Excel. "
        "Solo recibió metadata del Job y, si existe output_path, puede preparar la descarga."
    )

    with st.expander("Memoria de entrada en Workspace A"):
        st.json({
            "input_memory_info_before_upload": st.session_state["last_input_memory_info"],
            "input_zeroize_after_upload": st.session_state["last_input_zeroize_result"],
        })

    with st.expander("Resultado técnico del Job"):
        st.json(job_result)

    with st.expander("Latencias medidas"):
        st.json(st.session_state["last_latency"])

    latency = st.session_state["last_latency"]

    st.markdown("### Resumen de latencias")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Subida A → B",
            f"{latency['upload_a_to_b']['seconds']} s"
        )

    with col2:
        st.metric(
            "Mandar run_now",
            f"{latency['job_control_and_execution']['job_submit_api_seconds']} s"
        )

    with col3:
        st.metric(
            "Espera Job",
            f"{latency['job_control_and_execution']['job_wait_until_finished_seconds']} s"
        )

    col4, col5 = st.columns(2)

    with col4:
        st.metric(
            "Traer output JSON",
            f"{latency['job_control_and_execution']['job_output_fetch_seconds']} s"
        )

    with col5:
        st.metric(
            "Total hasta resultado",
            f"{latency['total_until_job_result_seconds']} s"
        )


# ============================================================
# UI: Preparar descarga del output
# ============================================================

output_path = st.session_state.get("last_output_path")
last_status = st.session_state.get("last_status")

if last_status == "success" and output_path:

    st.divider()
    st.subheader("Descarga del resultado")

    st.write("El archivo procesado está persistido en Workspace B:")
    st.code(output_path)

    if st.button("Preparar descarga desde Workspace B"):
        try:
            w = get_workspace_client()

            with st.spinner("Descargando output desde Workspace B hacia memoria de la app..."):
                output_buffer, download_latency = load_output_from_workspace_b(
                    w=w,
                    output_path=output_path,
                )

            output_memory_view = output_buffer.getbuffer()
            output_memory_info = get_memory_info(output_memory_view)
            output_memory_view.release()

            st.session_state["output_buffer"] = output_buffer
            st.session_state["output_file_name"] = os.path.basename(output_path)
            st.session_state["output_mime"] = infer_mime_type(output_path)
            st.session_state["output_memory_info"] = output_memory_info
            st.session_state["output_download_latency"] = download_latency

            st.rerun()

        except Exception as e:
            st.error("No se pudo preparar la descarga.")
            st.exception(e)


# ============================================================
# UI: Descargar archivo ya cargado en memoria
# ============================================================

if isinstance(st.session_state.get("output_buffer"), io.BytesIO):

    st.markdown("### Output cargado temporalmente en memoria de Workspace A")

    with st.expander("Memoria del output en Workspace A"):
        st.json(st.session_state["output_memory_info"])

    with st.expander("Latencia de descarga B → A"):
        st.json(st.session_state["output_download_latency"])

    st.metric(
        "Descarga B → A",
        f"{st.session_state['output_download_latency']['download_total_seconds']} s"
    )

    st.metric(
        "Throughput B → A",
        f"{st.session_state['output_download_latency']['download_throughput_mb_s']} MB/s"
    )

    st.caption(
        "Este buffer existe en memoria solo para servir la descarga al usuario. "
        "Después del click se intenta sobrescribir y liberar."
    )

    st.session_state["output_buffer"].seek(0)

    st.download_button(
        label="Descargar archivo procesado",
        data=st.session_state["output_buffer"],
        file_name=st.session_state["output_file_name"],
        mime=st.session_state["output_mime"],
        on_click=clear_download_buffer,
    )


# ============================================================
# UI: Resultado de limpieza post-descarga
# ============================================================

if st.session_state.get("download_cleanup_result") is not None:

    with st.expander("Limpieza del buffer de descarga"):
        st.json(st.session_state["download_cleanup_result"])