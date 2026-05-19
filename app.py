import ctypes
import gc
import hashlib
import json
import os
import time
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import streamlit as st
from databricks.sdk import WorkspaceClient


# ============================================================
# Configuración general de Streamlit
# ============================================================

st.set_page_config(
    page_title="Excel Cross-Workspace Processor",
    layout="wide"
)

st.title("Excel Cross-Workspace Processor")
st.caption(
    "La app recibe el Excel en memoria, lo envía al workspace destino, "
    "ejecuta un Job remoto y muestra el resultado sin persistir archivos en el workspace de la app."
)


# ============================================================
# Variables de entorno
# ============================================================

TARGET_WORKSPACE_HOST = os.getenv("TARGET_WORKSPACE_HOST")
TARGET_JOB_ID = os.getenv("TARGET_JOB_ID")
TARGET_TASK_KEY = os.getenv("TARGET_TASK_KEY", "Procesamiento_de_exceles")
TARGET_VOLUME_DIR = os.getenv("TARGET_VOLUME_DIR")

# Estas variables las inyecta Databricks Apps para la identidad de la app.
DATABRICKS_CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID")
DATABRICKS_CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")


# ============================================================
# Cliente Databricks contra el workspace destino
# ============================================================

def get_workspace_client() -> WorkspaceClient:
    """
    Crea un cliente contra el workspace destino.

    Importante:
    - Este host apunta al Workspace B, no necesariamente al workspace donde vive la app.
    - La autenticación usa el service principal de la app.
    - Ese service principal debe existir en Workspace B y tener permisos:
        - CAN MANAGE RUN sobre el Job
        - WRITE VOLUME sobre el Volume destino
        - READ VOLUME si también quieres descargar outputs
    """
    return WorkspaceClient(
        host=TARGET_WORKSPACE_HOST,
        client_id=DATABRICKS_CLIENT_ID,
        client_secret=DATABRICKS_CLIENT_SECRET,
    )


def validate_config() -> list[str]:
    """
    Valida que todas las variables necesarias existan.
    """
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
# Utilidades de archivo y seguridad básica
# ============================================================

def safe_filename(filename: str) -> str:
    """
    Limpia el nombre del archivo para evitar caracteres problemáticos en rutas.
    No cambia el contenido del archivo.
    """
    filename = os.path.basename(filename)
    filename = filename.replace(" ", "_")

    allowed_chars = []

    for char in filename:
        if char.isalnum() or char in [".", "_", "-"]:
            allowed_chars.append(char)
        else:
            allowed_chars.append("_")

    return "".join(allowed_chars)


def sha256_from_memoryview(buffer_view: memoryview) -> str:
    """
    Calcula hash SHA-256 directamente desde el buffer en memoria.
    No escribe nada a disco.
    """
    return hashlib.sha256(buffer_view).hexdigest()


def get_memory_pointer_info(uploaded_file) -> Tuple[Dict[str, Any], memoryview]:
    """
    Obtiene información diagnóstica del archivo en memoria.

    Notas importantes:
    - id(obj) identifica el objeto Python.
    - En CPython, id(obj) suele corresponder a una dirección de memoria del objeto,
      pero no debe tratarse como una garantía portable.
    - ctypes.addressof(...) intenta obtener la dirección virtual del buffer.
    - Esta dirección no es un path ni una ubicación navegable; solo sirve como evidencia
      diagnóstica de que estamos trabajando con un buffer en memoria del proceso.
    """

    buffer_view = uploaded_file.getbuffer()

    info = {
        "python_uploaded_file_object_id_hex": hex(id(uploaded_file)),
        "python_memoryview_object_id_hex": hex(id(buffer_view)),
        "buffer_size_bytes": buffer_view.nbytes,
        "buffer_readonly": buffer_view.readonly,
        "buffer_virtual_address_hex": None,
        "buffer_address_note": None,
    }

    try:
        if buffer_view.nbytes == 0:
            info["buffer_address_note"] = "El archivo está vacío; no hay dirección de buffer útil."
        elif buffer_view.readonly:
            info["buffer_address_note"] = (
                "El buffer es read-only; no se puede obtener dirección con ctypes.from_buffer."
            )
        else:
            address = ctypes.addressof(ctypes.c_char.from_buffer(buffer_view))
            info["buffer_virtual_address_hex"] = hex(address)
            info["buffer_address_note"] = (
                "Dirección virtual del buffer dentro del proceso Python. "
                "Es diagnóstica, no una garantía de control total sobre memoria."
            )

    except Exception as e:
        info["buffer_address_note"] = (
            f"No se pudo obtener dirección del buffer: {type(e).__name__}: {str(e)}"
        )

    return info, buffer_view


def zeroize_memoryview(buffer_view: memoryview) -> Dict[str, Any]:
    """
    Sobrescribe el buffer principal en memoria con ceros como best effort.

    Limitación:
    - Esto limpia el buffer expuesto por uploaded_file.getbuffer().
    - No garantiza que Streamlit, Python, el runtime o el sistema no hayan creado copias internas.
    - No sustituye controles de plataforma, auditoría o cifrado.
    """
    result = {
        "zeroize_attempted": True,
        "zeroize_success": False,
        "bytes_zeroized": 0,
        "error": None,
        "sample_after_zero_hex": None,
    }

    try:
        if buffer_view.readonly:
            result["error"] = "El buffer es read-only; no se puede sobrescribir."
            return result

        total_bytes = buffer_view.nbytes

        if total_bytes == 0:
            result["zeroize_success"] = True
            return result

        # Sobrescritura en chunks para evitar crear un objeto enorme de ceros.
        chunk_size = min(1024 * 1024, total_bytes)
        zero_chunk = b"\x00" * chunk_size

        for start in range(0, total_bytes, chunk_size):
            end = min(start + chunk_size, total_bytes)
            buffer_view[start:end] = zero_chunk[: end - start]

        result["zeroize_success"] = True
        result["bytes_zeroized"] = total_bytes

        sample_size = min(32, total_bytes)
        result["sample_after_zero_hex"] = bytes(buffer_view[:sample_size]).hex()

    except Exception as e:
        result["error"] = f"{type(e).__name__}: {str(e)}"

    return result


def local_persistence_probe(file_name: str) -> Dict[str, Any]:
    """
    Busca si accidentalmente se escribió un archivo local con el mismo nombre
    dentro del filesystem del contenedor de la app.

    Esto NO prueba ausencia absoluta de persistencia en toda la plataforma.
    Sí ayuda a demostrar que este código no escribió el Excel en ubicaciones
    locales típicas como:
    - directorio actual de la app
    - /tmp

    Para no hacer una búsqueda costosa, se limita la profundidad.
    """

    checked_roots = [
        os.getcwd(),
        "/tmp",
    ]

    matches = []

    for root in checked_roots:
        if not os.path.exists(root):
            continue

        root_path = Path(root)

        for current_root, _, files in os.walk(root):
            current_path = Path(current_root)

            try:
                depth = len(current_path.relative_to(root_path).parts)
            except Exception:
                depth = 0

            if depth > 3:
                continue

            if file_name in files:
                matches.append(str(current_path / file_name))

    return {
        "working_directory": os.getcwd(),
        "checked_roots": checked_roots,
        "searched_file_name": file_name,
        "local_file_matches": matches,
        "local_file_found": len(matches) > 0,
    }


# ============================================================
# Funciones contra Workspace B
# ============================================================

def upload_to_target_volume(
    w: WorkspaceClient,
    target_path: str,
    uploaded_file,
) -> None:
    """
    Sube el archivo al Unity Catalog Volume del Workspace B.

    Clave de diseño:
    - No usamos open(..., 'wb') local.
    - No escribimos en /tmp.
    - No guardamos bytes en st.session_state.
    - El archivo se transmite desde el objeto UploadedFile hacia la Files API.
    """

    uploaded_file.seek(0)

    w.files.upload(
        file_path=target_path,
        contents=uploaded_file,
        overwrite=True,
    )


def run_remote_excel_job(
    w: WorkspaceClient,
    input_path: str,
    run_id: str,
    original_file_name: str,
) -> Dict[str, Any]:
    """
    Ejecuta el Job remoto en Workspace B.

    El Job debe recibir:
    - input_path
    - run_id
    - original_file_name

    El Job debería devolver con dbutils.notebook.exit(...) un JSON tipo:
    {
      "status": "success",
      "processing_result": {...},
      "output_path": "/Volumes/.../output/<run_id>_transformed.xlsx",
      "audit_path": "/Volumes/.../audit/<run_id>_audit.json"
    }
    """

    waiter = w.jobs.run_now(
        job_id=int(TARGET_JOB_ID),
        notebook_params={
            "input_path": input_path,
            "run_id": run_id,
            "original_file_name": original_file_name,

            # Como tú sí quieres persistencia en Workspace B, NO pedimos borrar input.
            "delete_input": "false",
        },
    )

    run = waiter.result(timeout=timedelta(minutes=30))

    parent_run_id = run.run_id

    if not run.tasks:
        raise RuntimeError(
            f"El Job terminó, pero no se encontraron tasks. Parent run_id={parent_run_id}"
        )

    selected_task = None

    for task in run.tasks:
        if task.task_key == TARGET_TASK_KEY:
            selected_task = task
            break

    if selected_task is None:
        selected_task = run.tasks[0]

    task_run_id = selected_task.run_id

    output = w.jobs.get_run_output(run_id=task_run_id)

    if not output.notebook_output:
        raise RuntimeError(
            "El task no devolvió notebook_output. "
            "Valida que sea Notebook Task y que use dbutils.notebook.exit(...)."
        )

    raw_result = output.notebook_output.result

    if not raw_result:
        raise RuntimeError(
            "notebook_output.result está vacío. "
            "El notebook probablemente no ejecutó dbutils.notebook.exit(...)."
        )

    # Por seguridad, soportamos el prefijo visual que a veces aparece en logs.
    raw_result = raw_result.replace("Notebook exited:", "").strip()

    try:
        parsed_result = json.loads(raw_result)
    except json.JSONDecodeError:
        parsed_result = {
            "status": "raw_output_not_json",
            "raw_result": raw_result,
        }

    parsed_result["_job_metadata_from_app"] = {
        "parent_run_id": parent_run_id,
        "task_run_id": task_run_id,
        "selected_task_key": selected_task.task_key,
    }

    return parsed_result


def download_file_from_target_volume(
    w: WorkspaceClient,
    file_path: str,
) -> bytes:
    """
    Descarga un archivo desde el Volume del Workspace B hacia memoria de la app.

    Importante:
    - Esto no persiste el archivo en Workspace A.
    - El archivo queda como bytes en memoria solo para construir el botón de descarga.
    - No se escribe a disco local.
    """

    response = w.files.download(file_path=file_path)

    if not hasattr(response, "contents") or response.contents is None:
        raise RuntimeError(f"No se pudo descargar el archivo: {file_path}")

    try:
        return response.contents.read()
    finally:
        try:
            response.contents.close()
        except Exception:
            pass


def infer_mime_type(file_path: str) -> str:
    """
    Define MIME type básico según extensión.
    """
    lower_path = file_path.lower()

    if lower_path.endswith(".xlsx"):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    if lower_path.endswith(".csv"):
        return "text/csv"

    if lower_path.endswith(".json"):
        return "application/json"

    return "application/octet-stream"


def output_file_name_from_path(file_path: str) -> str:
    """
    Extrae nombre de archivo desde el path.
    """
    return os.path.basename(file_path)


# ============================================================
# UI: configuración
# ============================================================

st.subheader("1. Configuración detectada")

missing = validate_config()

config_preview = {
    "TARGET_WORKSPACE_HOST": TARGET_WORKSPACE_HOST,
    "TARGET_JOB_ID": TARGET_JOB_ID,
    "TARGET_TASK_KEY": TARGET_TASK_KEY,
    "TARGET_VOLUME_DIR": TARGET_VOLUME_DIR,
    "DATABRICKS_CLIENT_ID": DATABRICKS_CLIENT_ID,
    "DATABRICKS_CLIENT_SECRET": "***" if DATABRICKS_CLIENT_SECRET else None,
}

st.json(config_preview)

if missing:
    st.error(f"Faltan variables requeridas: {', '.join(missing)}")
    st.stop()


# ============================================================
# UI: carga de archivo
# ============================================================

st.subheader("2. Carga de Excel en memoria")

uploaded_file = st.file_uploader(
    "Carga un archivo Excel .xlsx",
    type=["xlsx"],
)

if uploaded_file is None:
    st.info("Carga un Excel para iniciar.")
    st.stop()


file_name = safe_filename(uploaded_file.name)

# Diagnóstico de memoria.
# Esto todavía no procesa el Excel. Solo inspecciona el buffer recibido por Streamlit.
memory_info, buffer_view = get_memory_pointer_info(uploaded_file)
file_hash = sha256_from_memoryview(buffer_view)

st.markdown("### Información del archivo recibido en memoria")

st.json({
    "file_name": file_name,
    "sha256": file_hash,
    **memory_info,
})

st.warning(
    "La dirección de memoria es diagnóstica. No prueba por sí sola ausencia total de copias internas. "
    "La garantía fuerte aquí es que el código de la app no escribe el archivo a disco ni lo guarda en session_state."
)


# ============================================================
# UI: ejecución
# ============================================================

st.subheader("3. Enviar a Workspace B y procesar")

if st.button("Procesar Excel en workspace destino", type="primary"):

    timings = {}
    run_id = str(uuid.uuid4())

    target_input_path = f"{TARGET_VOLUME_DIR}/input/{run_id}_{file_name}"

    st.markdown("### Política de persistencia de esta ejecución")

    st.json({
        "workspace_a_app_persistence": "No permitida por diseño del código",
        "workspace_b_job_persistence": "Permitida",
        "input_path_workspace_b": target_input_path,
        "file_bytes_stored_in_streamlit_session_state": False,
        "local_file_write_in_app": False,
    })

    local_probe_before = local_persistence_probe(file_name)

    st.markdown("### Evidencia antes de subir archivo")
    st.json({
        "local_persistence_probe_before_upload": local_probe_before,
    })

    t_total_start = time.perf_counter()

    try:
        w = get_workspace_client()

        # ------------------------------------------------------------
        # 1. Upload al Volume del Workspace B
        # ------------------------------------------------------------
        t_upload_start = time.perf_counter()

        with st.spinner("Subiendo archivo al Volume del workspace destino..."):
            upload_to_target_volume(
                w=w,
                target_path=target_input_path,
                uploaded_file=uploaded_file,
            )

        timings["upload_to_workspace_b_seconds"] = round(
            time.perf_counter() - t_upload_start,
            3,
        )

        st.success("Archivo subido al Volume del Workspace B.")

        # ------------------------------------------------------------
        # 2. Sobrescritura del buffer principal en memoria de Workspace A
        # ------------------------------------------------------------
        t_zero_start = time.perf_counter()

        zeroize_result = zeroize_memoryview(buffer_view)

        timings["zeroize_app_memory_buffer_seconds"] = round(
            time.perf_counter() - t_zero_start,
            3,
        )

        # Liberamos referencias explícitas.
        try:
            buffer_view.release()
        except Exception:
            pass

        # Importante: no guardamos uploaded_file ni bytes en session_state.
        gc.collect()

        st.markdown("### Limpieza best effort del buffer en memoria de la app")
        st.json(zeroize_result)

        # ------------------------------------------------------------
        # 3. Ejecutar Job remoto
        # ------------------------------------------------------------
        t_job_start = time.perf_counter()

        with st.spinner("Ejecutando Job remoto en Workspace B..."):
            job_result = run_remote_excel_job(
                w=w,
                input_path=target_input_path,
                run_id=run_id,
                original_file_name=file_name,
            )

        timings["remote_job_total_wait_seconds"] = round(
            time.perf_counter() - t_job_start,
            3,
        )

        timings["app_total_seconds"] = round(
            time.perf_counter() - t_total_start,
            3,
        )

        # ------------------------------------------------------------
        # 4. Evidencia posterior de no persistencia local
        # ------------------------------------------------------------
        local_probe_after = local_persistence_probe(file_name)

        st.markdown("### Evidencia después del procesamiento")
        st.json({
            "local_persistence_probe_after_processing": local_probe_after,
            "app_workspace_local_file_found": local_probe_after["local_file_found"],
            "interpretation": (
                "Si app_workspace_local_file_found=false, este código no dejó un archivo local "
                "con el nombre del Excel en las rutas inspeccionadas del contenedor de la app."
            ),
        })

        st.markdown("### Tiempos medidos desde la app")
        st.json(timings)

        # ------------------------------------------------------------
        # 5. Resultado del Job
        # ------------------------------------------------------------
        st.markdown("### Resultado crudo devuelto por el Job")
        st.json(job_result)

        status = job_result.get("status")

        if status == "success":
            st.success("El Job procesó el Excel correctamente en Workspace B.")

            processing = job_result.get("processing_result", {})

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

            st.markdown("### Columnas finales")
            st.write(processing.get("columns", []))

            st.markdown("### Vista previa del resultado")
            preview = processing.get("preview", [])

            if preview:
                st.dataframe(preview, use_container_width=True)
            else:
                st.info("El Job no devolvió preview en processing_result.preview.")

            st.markdown("### Perfil de columnas")
            profile = processing.get("profile", [])

            if profile:
                st.dataframe(profile, use_container_width=True)
            else:
                st.info("El Job no devolvió profile en processing_result.profile.")

            # --------------------------------------------------------
            # 6. Descargar output persistido en Workspace B
            # --------------------------------------------------------
            output_path = job_result.get("output_path")

            if output_path:
                st.markdown("### Output persistido en Workspace B")
                st.code(output_path)

                with st.spinner("Descargando output desde Workspace B hacia memoria de la app..."):
                    output_bytes = download_file_from_target_volume(
                        w=w,
                        file_path=output_path,
                    )

                st.download_button(
                    label="Descargar archivo procesado",
                    data=output_bytes,
                    file_name=output_file_name_from_path(output_path),
                    mime=infer_mime_type(output_path),
                )

                # Liberamos referencia al output descargado.
                del output_bytes
                gc.collect()

            else:
                st.warning(
                    "El Job fue exitoso, pero no devolvió output_path. "
                    "La app puede mostrar el preview, pero no puede ofrecer descarga del archivo procesado."
                )

            audit_path = job_result.get("audit_path")

            if audit_path:
                st.markdown("### Auditoría persistida en Workspace B")
                st.code(audit_path)

        elif status == "error":
            st.error("El Job terminó, pero el procesamiento interno falló.")
            st.write("Tipo de error:", job_result.get("error_type"))
            st.write("Mensaje:", job_result.get("error_message"))

        else:
            st.warning("El Job devolvió un estado no reconocido.")
            st.json(job_result)

    except Exception as e:
        st.error("Falló el flujo desde la app.")
        st.exception(e)

    finally:
        # Intentamos liberar referencias locales de la app.
        # Esto no garantiza borrado absoluto de RAM, pero reduce vida útil de referencias en Python.
        try:
            del uploaded_file
        except Exception:
            pass

        try:
            del buffer_view
        except Exception:
            pass

        gc.collect()