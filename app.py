"""
Backend API — Brücken Upload Portal
====================================
Flask API que recebe arquivos do portal web e envia para Azure Blob Storage.

Requisitos:
    pip install flask flask-cors azure-storage-blob python-dotenv

Variáveis de ambiente (.env):
    AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...
    AZURE_STORAGE_CONTAINER=uploads-clientes
    API_SECRET_KEY=sua-chave-secreta
    ALLOWED_ORIGINS=https://seu-dominio.com
"""

import os
import uuid
import hashlib
from datetime import datetime, timezone
from functools import wraps

from flask import Flask, request, jsonify
from flask_cors import CORS
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================
app = Flask(__name__)

CORS(app, origins=os.getenv('ALLOWED_ORIGINS', '*').split(','))

AZURE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
AZURE_CONTAINER = os.getenv('AZURE_STORAGE_CONTAINER', 'uploads-clientes')
API_SECRET_KEY = os.getenv('API_SECRET_KEY', 'dev-secret-key')

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'json', 'txt', 'parquet'}
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB

# Clientes autorizados (em produção, use um banco de dados)
AUTHORIZED_CLIENTS = {
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

# Histórico em memória (em produção, use Azure Table Storage ou SQL)
upload_history = {}

# ============================================================
# AZURE BLOB SERVICE
# ============================================================
blob_service_client = None

def get_blob_service():
    global blob_service_client
    if blob_service_client is None and AZURE_CONNECTION_STRING:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        # Cria o container se não existir
        try:
            blob_service_client.create_container(AZURE_CONTAINER)
        except Exception:
            pass  # Container já existe
    return blob_service_client


def upload_to_blob(file_stream, blob_name, content_type='application/octet-stream'):
    """Upload um arquivo para o Azure Blob Storage."""
    service = get_blob_service()
    if service is None:
        # Modo demo: simula upload
        return {
            'blob_name': blob_name,
            'url': f'https://demo.blob.core.windows.net/{AZURE_CONTAINER}/{blob_name}',
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
    """
    Gera caminho organizado no Blob Storage:
    {client_prefix}/{YYYY}/{MM}/{DD}/{uuid}_{filename}
    """
    now = datetime.now(timezone.utc)
    unique_id = uuid.uuid4().hex[:8]
    safe_name = original_filename.replace(' ', '_')
    return f"{client_prefix}/{now.year}/{now.month:02d}/{now.day:02d}/{unique_id}_{safe_name}"


# ============================================================
# ROUTES
# ============================================================

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({
        'status': 'ok',
        'storage_connected': get_blob_service() is not None,
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/api/auth', methods=['POST'])
def authenticate():
    """Autentica o cliente com ID + Token."""
    data = request.get_json()

    if not data:
        return jsonify({'error': 'Corpo da requisição vazio'}), 400

    client_id = data.get('client_id', '').strip()
    access_token = data.get('access_token', '').strip()

    if not client_id or not access_token:
        return jsonify({'error': 'ID do cliente e token são obrigatórios'}), 400

    client = AUTHORIZED_CLIENTS.get(client_id)
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
    """Recebe um arquivo e envia para o Azure Blob Storage."""

    # Valida presença do arquivo
    if 'file' not in request.files:
        return jsonify({'error': 'Nenhum arquivo enviado'}), 400

    file = request.files['file']
    client_id = request.form.get('client_id', '').strip()

    if not file.filename:
        return jsonify({'error': 'Nome do arquivo vazio'}), 400

    # Valida cliente
    client = AUTHORIZED_CLIENTS.get(client_id)
    if not client:
        return jsonify({'error': 'Cliente não autorizado'}), 401

    # Valida extensão
    if not allowed_file(file.filename):
        ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else 'N/A'
        return jsonify({'error': f'Extensão .{ext} não permitida'}), 400

    # Valida tamanho (lê o conteúdo para verificar)
    file_content = file.read()
    if len(file_content) > MAX_FILE_SIZE:
        return jsonify({'error': f'Arquivo excede o limite de {MAX_FILE_SIZE // (1024*1024)}MB'}), 400

    # Gera caminho no Blob Storage
    blob_path = generate_blob_path(client['container_prefix'], file.filename)

    # Determina content type
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
        from io import BytesIO
        result = upload_to_blob(
            BytesIO(file_content),
            blob_path,
            content_types.get(ext, 'application/octet-stream')
        )

        # Registra no histórico
        if client_id not in upload_history:
            upload_history[client_id] = []

        upload_history[client_id].insert(0, {
            'name': file.filename,
            'size': f'{len(file_content) / (1024*1024):.1f} MB' if len(file_content) > 1024*1024 else f'{len(file_content) / 1024:.1f} KB',
            'date': datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M'),
            'status': 'processed',
            'blob_path': blob_path
        })

        return jsonify({
            'success': True,
            'blob_path': blob_path,
            'file_name': file.filename,
            'file_size': len(file_content),
            'message': 'Arquivo enviado com sucesso para processamento'
        })

    except Exception as e:
        return jsonify({'error': f'Erro ao enviar arquivo: {str(e)}'}), 500


@app.route('/api/history', methods=['GET'])
def get_history():
    """Retorna o histórico de uploads do cliente."""
    client_id = request.args.get('client_id', '').strip()

    if not client_id:
        return jsonify({'error': 'client_id é obrigatório'}), 400

    history = upload_history.get(client_id, [])
    return jsonify(history)


# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("  Brücken Upload Portal — Backend API")
    print("=" * 60)
    print(f"  Azure Storage: {'Conectado' if AZURE_CONNECTION_STRING else 'MODO DEMO'}")
    print(f"  Container: {AZURE_CONTAINER}")
    print(f"  Clientes cadastrados: {len(AUTHORIZED_CLIENTS)}")
    print("=" * 60 + "\n")

    app.run(
        host='0.0.0.0',
        port=5000,
        debug=os.getenv('FLASK_DEBUG', 'true').lower() == 'true'
    )
