"""
Backend API — Brücken Upload Portal
====================================
Integrado ao ambiente credito-app-brucken existente.

Variáveis de ambiente EXISTENTES (não alterar):
    DB_SERVER       → srv-credito-analytics.database.windows.net
    DB_NAME         → db_credito
    DB_USER         → admin user
    DB_PASSWORD     → admin password
    JWT_SECRET      → chave secreta (reaproveitada para API auth)
    BIGDATA_API_KEY → API BigData Corp (motor de crédito)
    BIGDATA_TOKEN_ID → Token BigData Corp

Variáveis NOVAS (adicionar):
    AZURE_STORAGE_CONNECTION_STRING  → connection string do bruckencredito
    AZURE_STORAGE_CONTAINER          → uploads-clientes
    ALLOWED_ORIGINS                  → https://credito-app-brucken.azurewebsites.net
"""

import os
import uuid
import hashlib
import json
from datetime import datetime, timezone
from io import BytesIO

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG — usa nomes de variáveis JÁ EXISTENTES no App Service
# ============================================================
app = Flask(__name__)
CORS(app, origins=os.getenv('ALLOWED_ORIGINS', '*').split(','))

# Storage (NOVA)
AZURE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
AZURE_CONTAINER = os.getenv('AZURE_STORAGE_CONTAINER', 'uploads-clientes')

# SQL — variáveis que JÁ EXISTEM no credito-app-brucken
DB_SERVER = os.getenv('DB_SERVER', 'srv-credito-analytics.database.windows.net')
DB_NAME = os.getenv('DB_NAME', 'db_credito')
DB_USER = os.getenv('DB_USER', '')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

# Auth — reusa JWT_SECRET que já existe
API_SECRET_KEY = os.getenv('JWT_SECRET', 'dev-secret-key')

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'json', 'txt', 'parquet'}
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB


# ============================================================
# DATABASE CONNECTION — usa DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD
# ============================================================
def get_db_connection():
    """Conecta ao db_credito via variáveis existentes."""
    try:
        import pyodbc
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={DB_SERVER};"
            f"DATABASE={DB_NAME};"
            f"UID={DB_USER};"
            f"PWD={DB_PASSWORD};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        return pyodbc.connect(conn_str)
    except ImportError:
        app.logger.warning("pyodbc não instalado — modo demo ativo")
        return None
    except Exception as e:
        app.logger.error(f"Erro ao conectar ao SQL: {e}")
        return None


def query_db(sql, params=None, fetchone=False, commit=False):
    """Executa query no db_credito."""
    conn = get_db_connection()
    if conn is None:
        return None

    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        if commit:
            conn.commit()
            try:
                cursor.execute("SELECT SCOPE_IDENTITY()")
                row = cursor.fetchone()
                return row[0] if row else None
            except Exception:
                return None

        if fetchone:
            row = cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))
            return None
        else:
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        app.logger.error(f"Erro SQL: {e}")
        return None
    finally:
        conn.close()


# ============================================================
# FALLBACK — Clientes em memória (quando SQL não disponível)
# ============================================================
DEMO_CLIENTS = {
    'CLI-00123': {
        'token_hash': hashlib.sha256('token-secreto-123'.encode()).hexdigest(),
        'name': 'Empresa Demo LTDA',
        'container_prefix': 'cli-00123'
    },
    'CLI-00456': {
        'token_hash': hashlib.sha256('token-secreto-456'.encode()).hexdigest(),
        'name': 'Financeira ABC S.A.',
        'container_prefix': 'cli-00456'
    }
}

demo_upload_history = {}


# ============================================================
# AZURE BLOB SERVICE — bruckencredito
# ============================================================
blob_service_client = None


def get_blob_service():
    global blob_service_client
    if blob_service_client is None and AZURE_CONNECTION_STRING:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        try:
            blob_service_client.create_container(AZURE_CONTAINER)
        except Exception:
            pass
    return blob_service_client


def upload_to_blob(file_stream, blob_name, content_type='application/octet-stream'):
    """Upload para bruckencredito/uploads-clientes."""
    service = get_blob_service()
    if service is None:
        return {
            'blob_name': blob_name,
            'url': f'https://bruckencredito.blob.core.windows.net/{AZURE_CONTAINER}/{blob_name}',
            'size': 0,
            'demo': True
        }

    container_client = service.get_container_client(AZURE_CONTAINER)
    blob_client = container_client.get_blob_client(blob_name)

    content_settings = ContentSettings(content_type=content_type)
    blob_client.upload_blob(
        file_stream,
        overwrite=True,
        content_settings=content_settings,
        metadata={
            'uploaded_at': datetime.now(timezone.utc).isoformat(),
            'original_name': blob_name.split('/')[-1]
        }
    )

    return {
        'blob_name': blob_name,
        'url': blob_client.url,
        'size': blob_client.get_blob_properties().size
    }


# ============================================================
# HELPERS
# ============================================================
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def generate_blob_path(client_prefix, original_filename):
    now = datetime.now(timezone.utc)
    unique_id = uuid.uuid4().hex[:8]
    safe_name = original_filename.replace(' ', '_')
    return f"{client_prefix}/{now.year}/{now.month:02d}/{now.day:02d}/{unique_id}_{safe_name}"


def get_client(client_id):
    """Busca cliente no db_credito. Fallback para modo demo."""
    result = query_db(
        "SELECT client_id, client_name, token_hash, container_prefix, is_active "
        "FROM upload_clients WHERE client_id = ? AND is_active = 1",
        (client_id,),
        fetchone=True
    )

    if result:
        return {
            'token_hash': result['token_hash'],
            'name': result['client_name'],
            'container_prefix': result['container_prefix']
        }

    return DEMO_CLIENTS.get(client_id)


def register_upload_in_db(client_id, filename, blob_path, extension, size_bytes):
    """Registra upload na tabela upload_files."""
    file_id = query_db(
        """INSERT INTO upload_files
           (client_id, original_filename, blob_path, file_extension, file_size_bytes, upload_status)
           VALUES (?, ?, ?, ?, ?, 'uploaded')""",
        (client_id, filename, blob_path, extension, size_bytes),
        commit=True
    )
    return file_id


# ============================================================
# ROUTES
# ============================================================

@app.route('/')
def serve_frontend():
    # Em dev: ../frontend | Em deploy: ./frontend (mesma pasta)
    frontend_dir = os.path.join(os.path.dirname(__file__), '..', 'frontend')
    if not os.path.exists(frontend_dir):
        frontend_dir = os.path.join(os.path.dirname(__file__), 'frontend')
    return send_from_directory(frontend_dir, 'index.html')


@app.route('/api/health', methods=['GET'])
def health():
    """Health check — verifica Storage e SQL."""
    sql_ok = False
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            sql_ok = True
        except Exception:
            pass
        finally:
            conn.close()

    return jsonify({
        'status': 'ok',
        'storage_connected': get_blob_service() is not None,
        'sql_connected': sql_ok,
        'sql_server': DB_SERVER,
        'sql_database': DB_NAME,
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/api/auth', methods=['POST'])
def authenticate():
    """Autentica o cliente via upload_clients no db_credito."""
    data = request.get_json()

    if not data:
        return jsonify({'error': 'Corpo da requisição vazio'}), 400

    client_id = data.get('client_id', '').strip()
    access_token = data.get('access_token', '').strip()

    if not client_id or not access_token:
        return jsonify({'error': 'ID do cliente e token são obrigatórios'}), 400

    client = get_client(client_id)
    if not client:
        return jsonify({'error': 'Cliente não encontrado'}), 401

    token_hash = hashlib.sha256(access_token.encode()).hexdigest()
    if token_hash != client['token_hash']:
        return jsonify({'error': 'Token inválido'}), 401

    return jsonify({
        'authenticated': True,
        'client_name': client['name'],
        'client_id': client_id
    })


@app.route('/api/upload', methods=['POST'])
def upload_file():
    """Recebe arquivo → bruckencredito/uploads-clientes → registra em db_credito."""
    if 'file' not in request.files:
        return jsonify({'error': 'Nenhum arquivo enviado'}), 400

    file = request.files['file']
    client_id = request.form.get('client_id', '').strip()

    if not file.filename:
        return jsonify({'error': 'Nome do arquivo vazio'}), 400

    client = get_client(client_id)
    if not client:
        return jsonify({'error': 'Cliente não autorizado'}), 401

    if not allowed_file(file.filename):
        ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else 'N/A'
        return jsonify({'error': f'Extensão .{ext} não permitida'}), 400

    file_content = file.read()
    if len(file_content) > MAX_FILE_SIZE:
        return jsonify({'error': f'Arquivo excede o limite de {MAX_FILE_SIZE // (1024*1024)}MB'}), 400

    blob_path = generate_blob_path(client['container_prefix'], file.filename)
    ext = file.filename.rsplit('.', 1)[1].lower()

    content_types = {
        'csv': 'text/csv',
        'json': 'application/json',
        'txt': 'text/plain',
        'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'xls': 'application/vnd.ms-excel',
        'parquet': 'application/octet-stream'
    }

    try:
        result = upload_to_blob(
            BytesIO(file_content),
            blob_path,
            content_types.get(ext, 'application/octet-stream')
        )

        file_id = register_upload_in_db(
            client_id, file.filename, blob_path, ext, len(file_content)
        )

        return jsonify({
            'success': True,
            'file_id': file_id,
            'blob_path': blob_path,
            'file_name': file.filename,
            'file_size': len(file_content),
            'message': 'Arquivo enviado para bruckencredito e registrado em db_credito'
        })

    except Exception as e:
        return jsonify({'error': f'Erro ao enviar arquivo: {str(e)}'}), 500


@app.route('/api/history', methods=['GET'])
def get_history():
    """Retorna histórico de uploads do db_credito."""
    client_id = request.args.get('client_id', '').strip()
    if not client_id:
        return jsonify({'error': 'client_id é obrigatório'}), 400

    history = query_db(
        """SELECT TOP 50
               original_filename AS name,
               CASE
                   WHEN file_size_bytes >= 1048576
                   THEN CAST(CAST(file_size_bytes / 1048576.0 AS DECIMAL(10,1)) AS VARCHAR) + ' MB'
                   ELSE CAST(CAST(file_size_bytes / 1024.0 AS DECIMAL(10,1)) AS VARCHAR) + ' KB'
               END AS size,
               FORMAT(uploaded_at, 'dd/MM/yyyy HH:mm') AS date,
               upload_status AS status
           FROM upload_files
           WHERE client_id = ?
           ORDER BY uploaded_at DESC""",
        (client_id,)
    )

    if history is not None:
        return jsonify(history)

    return jsonify(demo_upload_history.get(client_id, []))


@app.route('/api/dashboard', methods=['GET'])
def dashboard():
    """Métricas do dashboard via vw_upload_dashboard."""
    result = query_db(
        "SELECT TOP 100 * FROM vw_upload_dashboard ORDER BY uploaded_at DESC"
    )
    return jsonify(result or [])


# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("  Brücken Upload Portal — Motor de Crédito")
    print("=" * 60)
    print(f"  Storage: bruckencredito / {AZURE_CONTAINER}")
    print(f"  SQL:     {DB_SERVER} / {DB_NAME}")
    print(f"  Blob:    {'Conectado' if AZURE_CONNECTION_STRING else 'MODO DEMO'}")
    print("=" * 60 + "\n")

    app.run(host='0.0.0.0', port=5000, debug=True)
