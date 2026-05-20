import gc
import io
import json
import os
import time
import uuid
from datetime import timedelta
from typing import Any, Dict, Optional, Tuple

import streamlit as st
from databricks.sdk import WorkspaceClient


# ============================================================
# Configuración general
# ============================================================

st.set_page_config(
    page_title="Excel Processor",
    layout="centered",
)

st.title("Procesador de Excel")
st.caption(
    "La app no abre ni procesa el Excel. Lo recibe temporalmente en memoria, "
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

OUTPUT_TTL_SECONDS = int(os.getenv("OUTPUT_TTL_SECONDS", "120"))


# ============================================================
# Estado de Streamlit
# ============================================================

def init_state() -> None:
    """
    Inicializa estado de sesión.

    Reglas:
    - No guardar el Excel de entrada en session_state.
    - Guardar únicamente metadata no sensible.
    - El output descargable vive temporalmente en memoria y tiene TTL.
    """

    defaults = {
        "uploader_version": 0,

        # Última ejecución
        "last_status": None,
        "last_run_id": None,
        "last_job_result": None,
        "last_output_path": None,
        "last_latency": None,
        "last_error": None,

        # Limpieza de input
        "last_input_zeroize_result": None,

        # Output temporal para descarga
        "output_buffer": None,
        "output_file_name": None,
        "output_mime": None,
        "output_created_at": None,
        "output_download_latency": None,
        "output_memory_info": None,

        # Limpieza de output
        "download_cleanup_result": None,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


init_state()


# ============================================================
# Utilidades de memoria
# ============================================================

def zeroize_memoryview(buffer_view: memoryview) -> Dict[str, Any]:
    """
    Sobrescribe con ceros el buffer controlado por el código.

    Limitación:
    esto NO garantiza borrado absoluto de copias internas de Streamlit, Python,
    SDK, HTTP stack o runtime. Es un control best effort.
    """

    result = {
        "zeroize_attempted": True,
        "zeroize_success": False,
        "bytes_zeroized": 0,
        "error_type": None,
    }

    try:
        if buffer_view.readonly:
            result["error_type"] = "ReadonlyBuffer"
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
        result["error_type"] = type(e).__name__

    return result


def zeroize_bytesio(buffer: io.BytesIO) -> Dict[str, Any]:
    """
    Limpia un BytesIO mutable.

    Se usa para el output descargable que vive temporalmente en memoria.
    """

    result = {
        "zeroize_attempted": True,
        "zeroize_success": False,
        "bytes_zeroized": 0,
        "error_type": None,
    }

    try:
        view = buffer.getbuffer()

        try:
            result = zeroize_memoryview(view)
        finally:
            view.release()

        buffer.close()

    except Exception as e:
        result["error_type"] = type(e).__name__

    return result


def clear_download_buffer() -> None:
    """
    Limpia el output temporal almacenado en memoria de Workspace A.

    Se ejecuta:
    - cuando el usuario da clic en descargar,
    - cuando expira el TTL,
    - o cuando el usuario presiona limpieza manual.
    """

    cleanup_result = {
        "cleanup_attempted": True,
        "zeroize_result": None,
    }

    output_buffer = st.session_state.get("output_buffer")

    if isinstance(output_buffer, io.BytesIO):
        cleanup_result["zeroize_result"] = zeroize_bytesio(output_buffer)

    st.session_state["output_buffer"] = None
    st.session_state["output_file_name"] = None
    st.session_state["output_mime"] = None
    st.session_state["output_created_at"] = None
    st.session_state["output_download_latency"] = None
    st.session_state["output_memory_info"] = None
    st.session_state["download_cleanup_result"] = cleanup_result

    gc.collect()


def apply_output_ttl_cleanup() -> None:
    """
    Limpia automáticamente el output si excede el TTL permitido en memoria.
    """

    output_buffer = st.session_state.get("output_buffer")
    created_at = st.session_state.get("output_created_at")

    if not isinstance(output_buffer, io.BytesIO) or created_at is None:
        return

    age_seconds = time.time() - created_at

    if age_seconds > OUTPUT_TTL_SECONDS:
        clear_download_buffer()


apply_output_ttl_cleanup()


# ============================================================
# Utilidades generales
# ============================================================

def get_workspace_client() -> WorkspaceClient:
    """
    Cliente contra Workspace B usando el service principal de la app.

    Permisos requeridos:
    - CAN MANAGE RUN sobre el Job.
    - WRITE VOLUME sobre input.
    - READ VOLUME sobre output.
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


def get_file_extension(filename: str) -> str:
    """
    Conserva solo la extensión.
    No usamos el nombre original en rutas ni parámetros.
    """

    _, ext = os.path.splitext(filename)
    ext = ext.lower()

    if ext != ".xlsx":
        raise ValueError("InvalidFileExtension")

    return ext


def infer_mime_type(file_path: str) -> str:
    lower = file_path.lower()

    if lower.endswith(".xlsx"):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    if lower.endswith(".csv"):
        return "text/csv"

    if lower.endswith(".json"):
        return "application/json"

    return "application/octet-stream"


def mb_per_second(size_bytes: int, seconds: float) -> Optional[float]:
    if seconds <= 0:
        return None

    return round((size_bytes / 1024 / 1024) / seconds, 4)


def register_sanitized_error(error: Exception, run_id: Optional[str] = None) -> str:
    """
    Manejo de error para producción.

    No imprime:
    - rutas,
    - nombres de archivo,
    - parámetros,
    - payloads,
    - str(error),
    - trazas completas.

    Esto evita exponer metadata sensible en UI/logs.
    """

    error_id = str(uuid.uuid4())

    log_payload = {
        "event": "app_error",
        "error_id": error_id,
        "error_type": type(error).__name__,
        "run_id": run_id,
    }

    print(json.dumps(log_payload, ensure_ascii=False))

    st.session_state["last_error"] = {
        "error_id": error_id,
        "error_type": type(error).__name__,
    }

    return error_id


def sanitize_job_result(job_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitiza la respuesta del Job para evitar traer datos del Excel a Workspace A.

    El Job NO debe devolver:
    - preview,
    - muestras de registros,
    - columnas con valores sensibles,
    - contenido del archivo,
    - errores con payloads.

    Solo se permite metadata técnica/controlada.
    """

    allowed_keys = {
        "status",
        "output_path",
        "error_code",
        "message",
        "rows_processed",
        "input_rows",
        "output_rows",
        "output_file_size_bytes",
    }

    sanitized = {}

    for key, value in job_result.items():
        if key not in allowed_keys:
            continue

        if isinstance(value, (str, int, float, bool)) or value is None:
            if isinstance(value, str):
                sanitized[key] = value[:500]
            else:
                sanitized[key] = value

    if "status" not in sanitized:
        sanitized["status"] = "unknown"

    return sanitized


def reset_uploader() -> None:
    """
    Reinicia el file_uploader para reducir la retención del input en el widget.
    """

    st.session_state["uploader_version"] += 1


# ============================================================
# Operaciones Workspace A -> Workspace B
# ============================================================

def upload_input_to_workspace_b(
    w: WorkspaceClient,
    uploaded_file,
    target_path: str,
    input_size_bytes: int,
) -> Dict[str, Any]:
    """
    Sube el Excel desde memoria de Workspace A al Volume de Workspace B.

    No escribe archivo local.
    No usa pandas/openpyxl.
    No abre el Excel.
    """

    uploaded_file.seek(0)

    t0 = time.perf_counter()

    w.files.upload(
        file_path=target_path,
        contents=uploaded_file,
        overwrite=True,
    )

    seconds = round(time.perf_counter() - t0, 3)

    return {
        "operation": "upload_input_a_to_b",
        "seconds": seconds,
        "uploaded_bytes": input_size_bytes,
        "upload_throughput_mb_s": mb_per_second(input_size_bytes, seconds),
    }


def run_remote_job(
    w: WorkspaceClient,
    input_path: str,
    run_id: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Ejecuta el Job remoto en Workspace B.

    No se envía original_file_name para evitar persistir nombres sensibles
    como metadata del run.

    delete_input=false queda fijo porque en esta arquitectura la persistencia
    está permitida en Workspace B - México.
    """

    latency = {}

    t_submit = time.perf_counter()

    waiter = w.jobs.run_now(
        job_id=int(TARGET_JOB_ID),
        notebook_params={
            "input_path": input_path,
            "run_id": run_id,
            "delete_input": "false",
        },
    )

    latency["job_submit_api_seconds"] = round(time.perf_counter() - t_submit, 3)

    t_wait = time.perf_counter()

    run = waiter.result(timeout=timedelta(minutes=30))

    latency["job_wait_until_finished_seconds"] = round(time.perf_counter() - t_wait, 3)
    latency["parent_run_id"] = run.run_id

    if not run.tasks:
        raise RuntimeError("JobWithoutTasks")

    selected_task = None

    for task in run.tasks:
        if task.task_key == TARGET_TASK_KEY:
            selected_task = task
            break

    if selected_task is None:
        selected_task = run.tasks[0]

    latency["selected_task_key"] = selected_task.task_key
    latency["task_run_id"] = selected_task.run_id

    t_output = time.perf_counter()

    output = w.jobs.get_run_output(run_id=selected_task.run_id)

    latency["job_output_fetch_seconds"] = round(time.perf_counter() - t_output, 3)

    if not output.notebook_output or not output.notebook_output.result:
        raise RuntimeError("EmptyNotebookOutput")

    raw_result = output.notebook_output.result.strip()

    parsed_result = json.loads(raw_result)
    sanitized_result = sanitize_job_result(parsed_result)

    return sanitized_result, latency


# ============================================================
# Operaciones Workspace B -> Workspace A
# ============================================================

def load_output_from_workspace_b(
    w: WorkspaceClient,
    output_path: str,
) -> Tuple[io.BytesIO, Dict[str, Any]]:
    """
    Descarga el output desde Workspace B hacia memoria de Workspace A.

    No escribe a disco.
    El buffer resultante tiene TTL y limpieza manual/on_click.
    """

    latency = {
        "operation": "download_output_b_to_a",
        "download_api_open_seconds": None,
        "download_stream_read_seconds": None,
        "download_total_seconds": None,
        "downloaded_bytes": 0,
        "download_throughput_mb_s": None,
    }

    t_total = time.perf_counter()

    t_open = time.perf_counter()
    response = w.files.download(file_path=output_path)
    latency["download_api_open_seconds"] = round(time.perf_counter() - t_open, 3)

    buffer = io.BytesIO()

    try:
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
        latency["downloaded_bytes"],
        latency["download_total_seconds"],
    )

    buffer.seek(0)

    return buffer, latency


# ============================================================
# UI: validación de configuración
# ============================================================

missing = validate_config()

if missing:
    st.error(f"Faltan variables de configuración: {', '.join(missing)}")
    st.stop()


with st.expander("Configuración técnica"):
    st.json({
        "target_workspace_configured": bool(TARGET_WORKSPACE_HOST),
        "target_job_id_configured": bool(TARGET_JOB_ID),
        "target_task_key": TARGET_TASK_KEY,
        "target_volume_dir_configured": bool(TARGET_VOLUME_DIR),
        "output_ttl_seconds": OUTPUT_TTL_SECONDS,
    })


# ============================================================
# UI: carga de archivo
# ============================================================

uploaded_file = st.file_uploader(
    "Carga un archivo Excel",
    type=["xlsx"],
    key=f"excel_uploader_{st.session_state['uploader_version']}",
)

if uploaded_file is not None:

    run_id = str(uuid.uuid4())
    input_zeroize_result = None
    buffer_view = None

    try:
        ext = get_file_extension(uploaded_file.name)
        buffer_view = uploaded_file.getbuffer()
        input_size_bytes = buffer_view.nbytes

        st.info(
            f"Archivo recibido temporalmente en memoria. "
            f"Tamaño: {round(input_size_bytes / 1024 / 1024, 4)} MB"
        )

        if st.button("Enviar a procesar", type="primary"):

            # No se usa nombre original en ruta.
            target_input_path = f"{TARGET_VOLUME_DIR}/input/{run_id}{ext}"

            full_latency = {
                "run_id": run_id,
                "input_size_bytes": input_size_bytes,
            }

            t_flow = time.perf_counter()

            try:
                w = get_workspace_client()

                # ----------------------------------------------------
                # 1. Upload A -> B
                # ----------------------------------------------------
                with st.spinner("Enviando archivo al workspace destino..."):
                    upload_latency = upload_input_to_workspace_b(
                        w=w,
                        uploaded_file=uploaded_file,
                        target_path=target_input_path,
                        input_size_bytes=input_size_bytes,
                    )

                full_latency["upload_a_to_b"] = upload_latency

                # ----------------------------------------------------
                # 2. Job remoto
                # ----------------------------------------------------
                with st.spinner("Procesando en el workspace destino..."):
                    job_result, job_latency = run_remote_job(
                        w=w,
                        input_path=target_input_path,
                        run_id=run_id,
                    )

                full_latency["job_control_and_execution"] = job_latency
                full_latency["total_until_job_result_seconds"] = round(
                    time.perf_counter() - t_flow,
                    3,
                )

                st.session_state["last_status"] = job_result.get("status")
                st.session_state["last_run_id"] = run_id
                st.session_state["last_job_result"] = job_result
                st.session_state["last_output_path"] = job_result.get("output_path")
                st.session_state["last_latency"] = full_latency
                st.session_state["last_error"] = None

            except Exception as e:
                error_id = register_sanitized_error(e, run_id=run_id)
                st.session_state["last_status"] = "app_error"
                st.session_state["last_run_id"] = run_id
                st.session_state["last_job_result"] = None
                st.session_state["last_output_path"] = None
                st.session_state["last_latency"] = full_latency
                st.session_state["last_error"] = {
                    "error_id": error_id,
                    "error_type": type(e).__name__,
                }

            finally:
                # ----------------------------------------------------
                # 3. Limpieza del input SIEMPRE, incluso si falla upload/job
                # ----------------------------------------------------
                try:
                    if buffer_view is not None:
                        input_zeroize_result = zeroize_memoryview(buffer_view)
                except Exception:
                    input_zeroize_result = {
                        "zeroize_attempted": True,
                        "zeroize_success": False,
                        "error_type": "UnhandledZeroizeError",
                    }

                try:
                    if buffer_view is not None:
                        buffer_view.release()
                except Exception:
                    pass

                try:
                    uploaded_file.close()
                except Exception:
                    pass

                st.session_state["last_input_zeroize_result"] = input_zeroize_result

                reset_uploader()
                gc.collect()
                st.rerun()

    except Exception as e:
        error_id = register_sanitized_error(e, run_id=run_id)
        st.error(f"No se pudo aceptar el archivo. ID de soporte: {error_id}")

        try:
            if buffer_view is not None:
                buffer_view.release()
        except Exception:
            pass

        try:
            uploaded_file.close()
        except Exception:
            pass

        reset_uploader()
        gc.collect()


# ============================================================
# UI: resultado de última ejecución
# ============================================================

if st.session_state["last_status"] is not None:

    st.divider()
    st.subheader("Última ejecución")

    status = st.session_state["last_status"]

    if status == "success":
        st.success("Archivo procesado correctamente en el workspace destino.")
    elif status == "app_error":
        error_info = st.session_state.get("last_error") or {}
        st.error(
            f"Falló el flujo de la app. ID de soporte: {error_info.get('error_id')}"
        )
    elif status == "error":
        st.error("El Job reportó error controlado.")
    else:
        st.warning(f"Estado reportado: {status}")

    if st.session_state.get("last_input_zeroize_result"):
        with st.expander("Limpieza del input en memoria"):
            st.json(st.session_state["last_input_zeroize_result"])

    if st.session_state.get("last_latency"):
        latency = st.session_state["last_latency"]

        st.markdown("### Resumen de latencias")

        col1, col2, col3 = st.columns(3)

        with col1:
            upload_seconds = latency.get("upload_a_to_b", {}).get("seconds")
            st.metric("Subida A → B", f"{upload_seconds} s")

        with col2:
            submit_seconds = latency.get("job_control_and_execution", {}).get(
                "job_submit_api_seconds"
            )
            st.metric("Mandar run_now", f"{submit_seconds} s")

        with col3:
            wait_seconds = latency.get("job_control_and_execution", {}).get(
                "job_wait_until_finished_seconds"
            )
            st.metric("Espera Job", f"{wait_seconds} s")

        col4, col5 = st.columns(2)

        with col4:
            fetch_seconds = latency.get("job_control_and_execution", {}).get(
                "job_output_fetch_seconds"
            )
            st.metric("Traer output JSON", f"{fetch_seconds} s")

        with col5:
            total_seconds = latency.get("total_until_job_result_seconds")
            st.metric("Total hasta resultado", f"{total_seconds} s")

        with st.expander("Detalle técnico de latencias"):
            st.json(latency)

    if st.session_state.get("last_job_result"):
        with st.expander("Resultado sanitizado del Job"):
            st.json(st.session_state["last_job_result"])


# ============================================================
# UI: descarga del output
# ============================================================

output_path = st.session_state.get("last_output_path")
last_status = st.session_state.get("last_status")

if last_status == "success" and output_path:

    st.divider()
    st.subheader("Descarga del resultado")

    st.write("El archivo procesado está disponible para descarga.")

    if st.button("Preparar descarga desde Workspace B"):
        try:
            w = get_workspace_client()

            with st.spinner("Cargando output temporalmente en memoria..."):
                output_buffer, download_latency = load_output_from_workspace_b(
                    w=w,
                    output_path=output_path,
                )

            st.session_state["output_buffer"] = output_buffer
            st.session_state["output_file_name"] = os.path.basename(output_path)
            st.session_state["output_mime"] = infer_mime_type(output_path)
            st.session_state["output_created_at"] = time.time()
            st.session_state["output_download_latency"] = download_latency
            st.session_state["output_memory_info"] = {
                "downloaded_bytes": download_latency.get("downloaded_bytes"),
                "ttl_seconds": OUTPUT_TTL_SECONDS,
            }

            st.rerun()

        except Exception as e:
            error_id = register_sanitized_error(
                e,
                run_id=st.session_state.get("last_run_id"),
            )
            st.error(f"No se pudo preparar la descarga. ID de soporte: {error_id}")


# ============================================================
# UI: archivo temporal listo para descarga
# ============================================================

if isinstance(st.session_state.get("output_buffer"), io.BytesIO):

    st.markdown("### Output temporal en memoria de Workspace A")

    created_at = st.session_state.get("output_created_at")
    age_seconds = round(time.time() - created_at, 1) if created_at else None
    remaining_seconds = (
        max(0, round(OUTPUT_TTL_SECONDS - age_seconds, 1))
        if age_seconds is not None
        else None
    )

    st.info(
        f"El archivo está cargado temporalmente en memoria para descarga. "
        f"TTL restante aproximado: {remaining_seconds} s."
    )

    with st.expander("Metadata del buffer de descarga"):
        st.json(st.session_state.get("output_memory_info"))

    with st.expander("Latencia B → A"):
        st.json(st.session_state.get("output_download_latency"))

    col1, col2 = st.columns(2)

    with col1:
        st.session_state["output_buffer"].seek(0)

        st.download_button(
            label="Descargar archivo procesado",
            data=st.session_state["output_buffer"],
            file_name=st.session_state["output_file_name"],
            mime=st.session_state["output_mime"],
            on_click=clear_download_buffer,
        )

    with col2:
        if st.button("Limpiar output de memoria"):
            clear_download_buffer()
            st.rerun()


# ============================================================
# UI: resultado de limpieza post-descarga
# ============================================================

if st.session_state.get("download_cleanup_result") is not None:

    with st.expander("Última limpieza del output"):
        st.json(st.session_state["download_cleanup_result"])