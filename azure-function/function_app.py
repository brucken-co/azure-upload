"""
Azure Function — Blob Trigger + Carga no db_credito
=====================================================
Disparada quando novo arquivo chega em bruckencredito/uploads-clientes.

Variáveis de ambiente (mesmos nomes do credito-app-brucken):
    AzureWebJobsStorage  → connection string do bruckencredito
    DB_SERVER            → srv-credito-analytics.database.windows.net
    DB_NAME              → db_credito
    DB_USER              → admin user
    DB_PASSWORD          → admin password
"""

import json
import logging
import os
from datetime import datetime, timezone
from io import BytesIO

import azure.functions as func
import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient

# ============================================================
# CONFIG — mesmos nomes de variáveis do credito-app-brucken
# ============================================================
CONNECTION_STRING = os.environ.get('AzureWebJobsStorage')
DB_SERVER = os.environ.get('DB_SERVER', 'srv-credito-analytics.database.windows.net')
DB_NAME = os.environ.get('DB_NAME', 'db_credito')
DB_USER = os.environ.get('DB_USER', '')
DB_PASSWORD = os.environ.get('DB_PASSWORD', '')

STAGING_CONTAINER = 'staging'
REJECTED_CONTAINER = 'rejected'
NOTIFICATIONS_CONTAINER = 'notifications'

VALIDATION_RULES = {
    'csv': {'max_rows': 10_000_000, 'max_columns': 500, 'encodings': ['utf-8', 'latin-1', 'iso-8859-1']},
    'xlsx': {'max_rows': 1_048_576, 'max_columns': 500, 'max_sheets': 50},
    'xls': {'max_rows': 1_048_576, 'max_columns': 500, 'max_sheets': 50},
    'json': {'max_size_mb': 200},
    'parquet': {'max_size_mb': 500},
    'txt': {'max_size_mb': 100},
}

SQL_BATCH_SIZE = 1000

app = func.FunctionApp()


# ============================================================
# DATABASE — usa DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD
# ============================================================
def get_sql_connection():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={DB_SERVER};DATABASE={DB_NAME};"
        f"UID={DB_USER};PWD={DB_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )


def update_upload_status(blob_path, status, validation_result):
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        cursor.execute(
            """UPDATE upload_files SET
                upload_status = ?,
                validation_errors = ?,
                validation_warnings = ?,
                file_metadata = ?,
                validated_at = GETUTCDATE()
            WHERE blob_path = ?""",
            (
                status,
                json.dumps(validation_result.get('errors', []), ensure_ascii=False),
                json.dumps(validation_result.get('warnings', []), ensure_ascii=False),
                json.dumps(validation_result.get('metadata', {}), ensure_ascii=False),
                blob_path
            )
        )
        conn.commit()
        conn.close()
        logging.info(f"upload_files atualizado: {blob_path} → {status}")
    except Exception as e:
        logging.error(f"Erro ao atualizar upload_files: {e}")


def get_upload_file_id(blob_path):
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, client_id FROM upload_files WHERE blob_path = ?", (blob_path,))
        row = cursor.fetchone()
        conn.close()
        return (row[0], row[1]) if row else (None, None)
    except Exception as e:
        logging.error(f"Erro ao buscar upload_file_id: {e}")
        return None, None


def load_dataframe_to_staging(df, upload_file_id, client_id):
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()

        total_rows = 0
        batch = []

        for idx, row in df.iterrows():
            row_data = row.to_json(force_ascii=False)
            batch.append((upload_file_id, client_id, idx + 1, row_data))

            if len(batch) >= SQL_BATCH_SIZE:
                cursor.executemany(
                    "INSERT INTO staging_data (upload_file_id, client_id, row_number, row_data) VALUES (?, ?, ?, ?)",
                    batch
                )
                total_rows += len(batch)
                batch = []

        if batch:
            cursor.executemany(
                "INSERT INTO staging_data (upload_file_id, client_id, row_number, row_data) VALUES (?, ?, ?, ?)",
                batch
            )
            total_rows += len(batch)

        cursor.execute(
            """UPDATE upload_files SET
                upload_status = 'loaded',
                rows_loaded = ?,
                loaded_at = GETUTCDATE()
            WHERE id = ?""",
            (total_rows, upload_file_id)
        )

        conn.commit()
        conn.close()
        logging.info(f"✓ {total_rows} linhas no staging_data (file_id={upload_file_id})")
        return total_rows

    except Exception as e:
        logging.error(f"Erro ao carregar staging: {e}")
        return 0


# ============================================================
# MAIN FUNCTION
# ============================================================
@app.blob_trigger(
    arg_name="blob",
    path="uploads-clientes/{name}",
    connection="AzureWebJobsStorage"
)
def process_uploaded_file(blob: func.InputStream):
    blob_name = blob.name
    blob_length = blob.length

    logging.info(f"[TRIGGER] Arquivo: {blob_name} ({blob_length} bytes)")

    relative_path = blob_name.replace('uploads-clientes/', '') if blob_name.startswith('uploads-clientes/') else blob_name
    parts = relative_path.split('/')
    filename = parts[-1] if parts else 'unknown'
    extension = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''

    upload_file_id, client_id = get_upload_file_id(relative_path)

    validation_result = {
        'blob_name': blob_name,
        'filename': filename,
        'extension': extension,
        'size_bytes': blob_length,
        'size_mb': round(blob_length / (1024 * 1024), 2),
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'valid': False,
        'errors': [],
        'warnings': [],
        'metadata': {}
    }

    try:
        content = blob.read()
        df = None

        if extension == 'csv':
            validation_result, df = validate_csv(content, validation_result)
        elif extension in ('xlsx', 'xls'):
            validation_result, df = validate_excel(content, validation_result)
        elif extension == 'json':
            validation_result, df = validate_json(content, validation_result)
        elif extension == 'parquet':
            validation_result, df = validate_parquet(content, validation_result)
        elif extension == 'txt':
            validation_result['metadata'] = validate_txt(content)
        else:
            validation_result['errors'].append(f'Extensão não suportada: .{extension}')

        if not validation_result['errors']:
            validation_result['valid'] = True
            move_blob(content, blob_name, STAGING_CONTAINER)
            update_upload_status(relative_path, 'staged', validation_result)

            if df is not None and upload_file_id:
                rows = load_dataframe_to_staging(df, upload_file_id, client_id)
                logging.info(f"[OK] {blob_name} → staging + {rows} linhas no db_credito")
            else:
                logging.info(f"[OK] {blob_name} → staging")
        else:
            move_blob(content, blob_name, REJECTED_CONTAINER)
            update_upload_status(relative_path, 'rejected', validation_result)
            logging.warning(f"[REJEITADO] {blob_name}: {validation_result['errors']}")

        save_notification(validation_result)

    except Exception as e:
        logging.error(f"[ERRO] {blob_name}: {str(e)}")
        validation_result['errors'].append(f'Erro interno: {str(e)}')
        update_upload_status(relative_path, 'error', validation_result)
        save_notification(validation_result)


# ============================================================
# VALIDATORS
# ============================================================
def validate_csv(content, result):
    rules = VALIDATION_RULES['csv']
    df = None

    for enc in rules['encodings']:
        try:
            df = pd.read_csv(BytesIO(content), encoding=enc, low_memory=False)
            result['metadata']['encoding'] = enc
            break
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue

    if df is None:
        result['errors'].append('CSV ilegível com encodings UTF-8/Latin-1')
        return result, None

    rows, cols = df.shape
    if rows > rules['max_rows']:
        result['errors'].append(f'Excede {rules["max_rows"]:,} linhas ({rows:,})')
    if cols > rules['max_columns']:
        result['errors'].append(f'Excede {rules["max_columns"]} colunas ({cols})')

    dup_cols = df.columns[df.columns.duplicated()].tolist()
    if dup_cols:
        result['warnings'].append(f'Colunas duplicadas: {dup_cols[:5]}')

    empty = df.isnull().all(axis=1).sum()
    if empty > 0:
        result['warnings'].append(f'{empty} linhas vazias')

    result['metadata'].update({
        'rows': rows, 'columns': cols,
        'column_names': df.columns.tolist()[:50],
        'null_pct': round(df.isnull().mean().mean() * 100, 2)
    })

    return result, df


def validate_excel(content, result):
    rules = VALIDATION_RULES['xlsx']

    try:
        xls = pd.ExcelFile(BytesIO(content))
    except Exception as e:
        result['errors'].append(f'Excel corrompido: {str(e)}')
        return result, None

    sheets = xls.sheet_names
    if len(sheets) > rules['max_sheets']:
        result['errors'].append(f'Excede {rules["max_sheets"]} abas ({len(sheets)})')
        return result, None

    try:
        df = pd.read_excel(xls, sheet_name=0)
    except Exception as e:
        result['errors'].append(f'Erro ao ler primeira aba: {str(e)}')
        return result, None

    rows, cols = df.shape
    if rows > rules['max_rows']:
        result['errors'].append(f'Excede {rules["max_rows"]:,} linhas ({rows:,})')
    if cols > rules['max_columns']:
        result['errors'].append(f'Excede {rules["max_columns"]} colunas ({cols})')

    result['metadata'] = {
        'sheet_count': len(sheets), 'sheet_names': sheets,
        'rows': rows, 'columns': cols,
        'column_names': df.columns.tolist()[:50]
    }

    return result, df


def validate_json(content, result):
    rules = VALIDATION_RULES['json']
    size_mb = len(content) / (1024 * 1024)

    if size_mb > rules['max_size_mb']:
        result['errors'].append(f'Excede {rules["max_size_mb"]}MB ({size_mb:.1f}MB)')
        return result, None

    try:
        data = json.loads(content.decode('utf-8'))
    except UnicodeDecodeError:
        data = json.loads(content.decode('latin-1'))
    except json.JSONDecodeError as e:
        result['errors'].append(f'JSON inválido: {str(e)}')
        return result, None

    df = None
    if isinstance(data, list):
        df = pd.DataFrame(data)
        result['metadata'] = {'type': 'array', 'records': len(data)}
    elif isinstance(data, dict):
        result['metadata'] = {'type': 'object', 'keys': list(data.keys())[:20]}

    return result, df


def validate_parquet(content, result):
    rules = VALIDATION_RULES['parquet']
    size_mb = len(content) / (1024 * 1024)

    if size_mb > rules['max_size_mb']:
        result['errors'].append(f'Excede {rules["max_size_mb"]}MB ({size_mb:.1f}MB)')
        return result, None

    try:
        df = pd.read_parquet(BytesIO(content))
        rows, cols = df.shape
        result['metadata'] = {
            'rows': rows, 'columns': cols,
            'column_names': df.columns.tolist()[:50]
        }
        return result, df
    except Exception as e:
        result['errors'].append(f'Parquet inválido: {str(e)}')
        return result, None


def validate_txt(content):
    try:
        text = content.decode('utf-8')
    except UnicodeDecodeError:
        text = content.decode('latin-1')
    return {'lines': text.count('\n') + 1, 'characters': len(text)}


# ============================================================
# BLOB OPERATIONS
# ============================================================
def move_blob(content, original_path, dest_container):
    if not CONNECTION_STRING:
        logging.info(f"[DEMO] Move {original_path} → {dest_container}")
        return

    try:
        service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        try:
            service.create_container(dest_container)
        except Exception:
            pass

        dest_name = original_path.replace('uploads-clientes/', '')
        blob_client = service.get_container_client(dest_container).get_blob_client(dest_name)
        blob_client.upload_blob(content, overwrite=True)

        orig_name = original_path.replace('uploads-clientes/', '')
        service.get_container_client('uploads-clientes').get_blob_client(orig_name).delete_blob()

        logging.info(f"Movido: uploads-clientes/{orig_name} → {dest_container}/{dest_name}")
    except Exception as e:
        logging.error(f"Erro ao mover blob: {e}")


def save_notification(result):
    if not CONNECTION_STRING:
        return
    try:
        service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        try:
            service.create_container(NOTIFICATIONS_CONTAINER)
        except Exception:
            pass

        now = datetime.now(timezone.utc)
        report_name = f"{now.strftime('%Y/%m/%d')}/{result['filename']}_report.json"
        blob_client = service.get_container_client(NOTIFICATIONS_CONTAINER).get_blob_client(report_name)
        blob_client.upload_blob(
            json.dumps(result, indent=2, ensure_ascii=False).encode('utf-8'),
            overwrite=True
        )
    except Exception as e:
        logging.error(f"Erro ao salvar notificação: {e}")
