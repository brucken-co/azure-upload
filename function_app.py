"""
Azure Function — Blob Trigger para validação e processamento
==============================================================
Esta function é disparada automaticamente quando um novo arquivo
é carregado no container 'uploads-clientes' do Azure Blob Storage.

Fluxo:
    1. Novo blob detectado via BlobTrigger
    2. Valida formato e integridade do arquivo
    3. Se válido → move para container 'staging' e notifica pipeline
    4. Se inválido → move para container 'rejected' e notifica cliente

Deploy:
    func azure functionapp publish <FUNCTION_APP_NAME>

Requisitos (requirements.txt):
    azure-functions
    azure-storage-blob
    pandas
    openpyxl
"""

import json
import logging
import os
from datetime import datetime, timezone
from io import BytesIO

import azure.functions as func
import pandas as pd
from azure.storage.blob import BlobServiceClient

# ============================================================
# CONFIG
# ============================================================
CONNECTION_STRING = os.environ.get('AzureWebJobsStorage')
STAGING_CONTAINER = 'staging'
REJECTED_CONTAINER = 'rejected'
NOTIFICATIONS_CONTAINER = 'notifications'

# Regras de validação por tipo de arquivo
VALIDATION_RULES = {
    'csv': {
        'max_rows': 10_000_000,
        'max_columns': 500,
        'required_encoding': ['utf-8', 'latin-1', 'iso-8859-1'],
    },
    'xlsx': {
        'max_rows': 1_048_576,
        'max_columns': 500,
        'max_sheets': 50,
    },
    'json': {
        'max_size_mb': 200,
    },
    'parquet': {
        'max_size_mb': 500,
    },
    'txt': {
        'max_size_mb': 100,
    }
}


# ============================================================
# MAIN FUNCTION
# ============================================================
app = func.FunctionApp()


@app.blob_trigger(
    arg_name="blob",
    path="uploads-clientes/{client_prefix}/{year}/{month}/{day}/{filename}",
    connection="AzureWebJobsStorage"
)
def process_uploaded_file(blob: func.InputStream):
    """
    Processa automaticamente arquivos enviados pelo portal de upload.
    """
    blob_name = blob.name
    blob_length = blob.length

    logging.info(f"[TRIGGER] Novo arquivo detectado: {blob_name} ({blob_length} bytes)")

    # Extrai metadados do path
    parts = blob_name.replace('uploads-clientes/', '').split('/')
    client_prefix = parts[0] if len(parts) > 0 else 'unknown'
    filename = parts[-1] if len(parts) > 0 else 'unknown'
    extension = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''

    validation_result = {
        'blob_name': blob_name,
        'client': client_prefix,
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
        # Lê o conteúdo do blob
        content = blob.read()

        # Valida por tipo
        if extension == 'csv':
            validation_result = validate_csv(content, validation_result)
        elif extension in ('xlsx', 'xls'):
            validation_result = validate_excel(content, validation_result)
        elif extension == 'json':
            validation_result = validate_json(content, validation_result)
        elif extension == 'parquet':
            validation_result = validate_parquet(content, validation_result)
        elif extension == 'txt':
            validation_result = validate_txt(content, validation_result)
        else:
            validation_result['errors'].append(f'Extensão não suportada: .{extension}')

        # Decide destino
        if not validation_result['errors']:
            validation_result['valid'] = True
            move_blob(content, blob_name, STAGING_CONTAINER, validation_result)
            logging.info(f"[OK] Arquivo válido, movido para staging: {blob_name}")
        else:
            move_blob(content, blob_name, REJECTED_CONTAINER, validation_result)
            logging.warning(f"[REJEITADO] {blob_name}: {validation_result['errors']}")

        # Salva relatório de validação
        save_validation_report(validation_result)

    except Exception as e:
        logging.error(f"[ERRO] Falha ao processar {blob_name}: {str(e)}")
        validation_result['errors'].append(f'Erro interno: {str(e)}')
        save_validation_report(validation_result)


# ============================================================
# VALIDATORS
# ============================================================
def validate_csv(content: bytes, result: dict) -> dict:
    """Valida arquivo CSV."""
    rules = VALIDATION_RULES['csv']

    # Tenta detectar encoding
    encoding_used = None
    df = None

    for enc in rules['required_encoding']:
        try:
            df = pd.read_csv(BytesIO(content), encoding=enc, nrows=5)
            encoding_used = enc
            break
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue

    if df is None:
        result['errors'].append('Não foi possível ler o CSV com encodings suportados (UTF-8, Latin-1)')
        return result

    # Lê o arquivo completo para contagem
    try:
        df_full = pd.read_csv(BytesIO(content), encoding=encoding_used, low_memory=False)
    except Exception as e:
        result['errors'].append(f'Erro ao parsear CSV: {str(e)}')
        return result

    rows, cols = df_full.shape

    if rows > rules['max_rows']:
        result['errors'].append(f'CSV excede limite de {rules["max_rows"]:,} linhas ({rows:,} encontradas)')

    if cols > rules['max_columns']:
        result['errors'].append(f'CSV excede limite de {rules["max_columns"]} colunas ({cols} encontradas)')

    # Verifica colunas duplicadas
    duplicated_cols = df_full.columns[df_full.columns.duplicated()].tolist()
    if duplicated_cols:
        result['warnings'].append(f'Colunas duplicadas encontradas: {duplicated_cols[:5]}')

    # Verifica linhas completamente vazias
    empty_rows = df_full.isnull().all(axis=1).sum()
    if empty_rows > 0:
        result['warnings'].append(f'{empty_rows} linhas completamente vazias encontradas')

    # Metadata
    result['metadata'] = {
        'encoding': encoding_used,
        'rows': rows,
        'columns': cols,
        'column_names': df_full.columns.tolist()[:50],  # Limita a 50 para o relatório
        'dtypes': {col: str(dtype) for col, dtype in df_full.dtypes.items()},
        'null_percentage': round(df_full.isnull().mean().mean() * 100, 2),
        'memory_usage_mb': round(df_full.memory_usage(deep=True).sum() / (1024 * 1024), 2)
    }

    return result


def validate_excel(content: bytes, result: dict) -> dict:
    """Valida arquivo Excel."""
    rules = VALIDATION_RULES['xlsx']

    try:
        xls = pd.ExcelFile(BytesIO(content))
    except Exception as e:
        result['errors'].append(f'Arquivo Excel corrompido ou ilegível: {str(e)}')
        return result

    sheet_names = xls.sheet_names

    if len(sheet_names) > rules['max_sheets']:
        result['errors'].append(f'Excel excede limite de {rules["max_sheets"]} abas ({len(sheet_names)} encontradas)')
        return result

    total_rows = 0
    sheets_info = {}

    for sheet in sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet)
            rows, cols = df.shape
            total_rows += rows

            if cols > rules['max_columns']:
                result['errors'].append(f'Aba "{sheet}" excede {rules["max_columns"]} colunas ({cols})')

            sheets_info[sheet] = {
                'rows': rows,
                'columns': cols,
                'column_names': df.columns.tolist()[:20]
            }
        except Exception as e:
            result['warnings'].append(f'Erro ao ler aba "{sheet}": {str(e)}')

    if total_rows > rules['max_rows']:
        result['errors'].append(f'Total de linhas ({total_rows:,}) excede o limite ({rules["max_rows"]:,})')

    result['metadata'] = {
        'sheet_count': len(sheet_names),
        'sheet_names': sheet_names,
        'total_rows': total_rows,
        'sheets_detail': sheets_info
    }

    return result


def validate_json(content: bytes, result: dict) -> dict:
    """Valida arquivo JSON."""
    rules = VALIDATION_RULES['json']

    size_mb = len(content) / (1024 * 1024)
    if size_mb > rules['max_size_mb']:
        result['errors'].append(f'JSON excede limite de {rules["max_size_mb"]}MB ({size_mb:.1f}MB)')
        return result

    try:
        data = json.loads(content.decode('utf-8'))
    except UnicodeDecodeError:
        try:
            data = json.loads(content.decode('latin-1'))
        except Exception as e:
            result['errors'].append(f'Encoding não suportado: {str(e)}')
            return result
    except json.JSONDecodeError as e:
        result['errors'].append(f'JSON inválido: {str(e)}')
        return result

    # Metadata
    if isinstance(data, list):
        result['metadata'] = {
            'type': 'array',
            'record_count': len(data),
            'sample_keys': list(data[0].keys())[:20] if data and isinstance(data[0], dict) else []
        }
    elif isinstance(data, dict):
        result['metadata'] = {
            'type': 'object',
            'top_level_keys': list(data.keys())[:20]
        }

    return result


def validate_parquet(content: bytes, result: dict) -> dict:
    """Valida arquivo Parquet."""
    rules = VALIDATION_RULES['parquet']

    size_mb = len(content) / (1024 * 1024)
    if size_mb > rules['max_size_mb']:
        result['errors'].append(f'Parquet excede limite de {rules["max_size_mb"]}MB ({size_mb:.1f}MB)')
        return result

    try:
        df = pd.read_parquet(BytesIO(content))
        rows, cols = df.shape

        result['metadata'] = {
            'rows': rows,
            'columns': cols,
            'column_names': df.columns.tolist()[:50],
            'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
        }
    except Exception as e:
        result['errors'].append(f'Parquet inválido ou corrompido: {str(e)}')

    return result


def validate_txt(content: bytes, result: dict) -> dict:
    """Valida arquivo TXT."""
    rules = VALIDATION_RULES['txt']

    size_mb = len(content) / (1024 * 1024)
    if size_mb > rules['max_size_mb']:
        result['errors'].append(f'TXT excede limite de {rules["max_size_mb"]}MB ({size_mb:.1f}MB)')
        return result

    try:
        text = content.decode('utf-8')
    except UnicodeDecodeError:
        try:
            text = content.decode('latin-1')
        except Exception:
            result['errors'].append('Encoding não suportado')
            return result

    lines = text.count('\n') + 1
    result['metadata'] = {
        'lines': lines,
        'characters': len(text),
        'encoding': 'utf-8'
    }

    return result


# ============================================================
# BLOB OPERATIONS
# ============================================================
def move_blob(content: bytes, original_path: str, destination_container: str, metadata: dict):
    """Move o blob para o container de destino (staging ou rejected)."""
    if not CONNECTION_STRING:
        logging.info(f"[DEMO] Simulando move para {destination_container}/{original_path}")
        return

    try:
        service = BlobServiceClient.from_connection_string(CONNECTION_STRING)

        # Cria container destino se necessário
        try:
            service.create_container(destination_container)
        except Exception:
            pass

        # Upload no destino
        dest_blob_name = original_path.replace('uploads-clientes/', '')
        container_client = service.get_container_client(destination_container)
        blob_client = container_client.get_blob_client(dest_blob_name)

        blob_client.upload_blob(
            content,
            overwrite=True,
            metadata={
                'validation_status': 'valid' if metadata.get('valid') else 'rejected',
                'validated_at': datetime.now(timezone.utc).isoformat(),
                'original_path': original_path
            }
        )

        # Remove do container original
        original_container = 'uploads-clientes'
        original_blob = service.get_blob_client(original_container, original_path.replace(f'{original_container}/', ''))
        original_blob.delete_blob()

        logging.info(f"Blob movido: {original_path} → {destination_container}/{dest_blob_name}")

    except Exception as e:
        logging.error(f"Erro ao mover blob: {str(e)}")


def save_validation_report(result: dict):
    """Salva o relatório de validação como JSON no Blob Storage."""
    if not CONNECTION_STRING:
        logging.info(f"[DEMO] Relatório: {json.dumps(result, indent=2, ensure_ascii=False)}")
        return

    try:
        service = BlobServiceClient.from_connection_string(CONNECTION_STRING)

        try:
            service.create_container(NOTIFICATIONS_CONTAINER)
        except Exception:
            pass

        report_name = f"{result['client']}/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{result['filename']}_report.json"

        container_client = service.get_container_client(NOTIFICATIONS_CONTAINER)
        blob_client = container_client.get_blob_client(report_name)

        blob_client.upload_blob(
            json.dumps(result, indent=2, ensure_ascii=False).encode('utf-8'),
            overwrite=True
        )

        logging.info(f"Relatório salvo: {report_name}")

    except Exception as e:
        logging.error(f"Erro ao salvar relatório: {str(e)}")
