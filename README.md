# Brücken Upload Portal

Portal seguro para ingestão de bases de dados de clientes com processamento automatizado no Azure.

## Arquitetura

```
┌─────────────────┐     HTTPS/TLS 1.3      ┌──────────────────────┐
│                  │ ──────────────────────► │                      │
│  Cliente (Web)   │     Upload de arquivo   │  Backend Flask       │
│  index.html      │ ◄────────────────────── │  (App Service)       │
│                  │     Status + Histórico  │                      │
└─────────────────┘                          └──────────┬───────────┘
                                                        │
                                                        │ Azure SDK
                                                        ▼
                                             ┌──────────────────────┐
                                             │  Azure Blob Storage  │
                                             │  uploads-clientes/   │
                                             └──────────┬───────────┘
                                                        │
                                                        │ Blob Trigger
                                                        ▼
                                             ┌──────────────────────┐
                                             │  Azure Function      │
                                             │  (Validação)         │
                                             └──────┬─────┬─────────┘
                                                    │     │
                                          Válido ───┘     └─── Inválido
                                                    │             │
                                                    ▼             ▼
                                             ┌───────────┐ ┌────────────┐
                                             │  staging/  │ │ rejected/  │
                                             └─────┬─────┘ └────────────┘
                                                   │
                                                   │ Data Factory
                                                   ▼
                                             ┌───────────────────┐
                                             │  Azure SQL /      │
                                             │  Synapse Analytics │
                                             └───────────────────┘
```

## Estrutura do Projeto

```
upload-portal/
├── frontend/
│   └── index.html          # Portal web (HTML/CSS/JS standalone)
├── backend/
│   ├── app.py              # API Flask (auth, upload, histórico)
│   ├── requirements.txt
│   └── .env.example        # Template de variáveis de ambiente
├── azure-function/
│   ├── function_app.py     # Blob Trigger (validação automática)
│   ├── requirements.txt
│   └── host.json
├── deploy.sh               # Script de deploy da infraestrutura
└── README.md
```

## Setup Rápido

### 1. Infraestrutura Azure

```bash
# Login no Azure
az login

# Rodar o script de deploy
chmod +x deploy.sh
./deploy.sh
```

### 2. Backend (Flask)

```bash
cd backend

# Criar e configurar .env
cp .env.example .env
# Edite o .env com sua connection string

# Instalar dependências
pip install -r requirements.txt

# Rodar localmente
python app.py

# Deploy no Azure
az webapp up --name app-brucken-upload
```

### 3. Azure Function

```bash
cd azure-function

# Instalar Azure Functions Core Tools
npm install -g azure-functions-core-tools@4

# Testar localmente
func start

# Deploy
func azure functionapp publish func-brucken-upload
```

### 4. Frontend

O `index.html` pode ser hospedado de várias formas:

- **Azure Static Web Apps** (recomendado)
- **Azure Blob Storage** com static website
- **Junto com o backend** em `/static/`
- **GitHub Pages** (seu domínio custom)

## Segurança

| Camada | Implementação |
|--------|--------------|
| Transporte | TLS 1.3 obrigatório |
| Autenticação | Client ID + Token (hash SHA-256) |
| Storage | Encryption at rest (AES-256) |
| Rede | Public access desabilitado no Blob |
| Validação | Tipo, tamanho e integridade na Function |
| CORS | Origins restritas |

### Para produção, adicione:

- **Azure AD / Entra ID** para autenticação do cliente
- **Private Endpoint** no Storage Account
- **WAF (Web Application Firewall)** no App Gateway
- **Azure Key Vault** para tokens e connection strings
- **Managed Identity** para comunicação entre serviços

## Containers do Blob Storage

| Container | Finalidade |
|-----------|-----------|
| `uploads-clientes` | Recebe os arquivos do portal |
| `staging` | Arquivos validados prontos para processamento |
| `rejected` | Arquivos inválidos (com relatório de erros) |
| `notifications` | Relatórios de validação (JSON) |

## Formatos Suportados

- CSV (UTF-8, Latin-1)
- Excel (.xlsx, .xls)
- JSON
- Parquet
- TXT

Limite: 500 MB por arquivo.

## Custos Estimados (Brazil South)

| Recurso | SKU | Estimativa/mês |
|---------|-----|----------------|
| Storage Account | Standard LRS | ~R$ 5-20 (depende do volume) |
| App Service | B1 Linux | ~R$ 70 |
| Function App | Consumption | ~R$ 0-5 (pay per execution) |
| Event Grid | Standard | ~R$ 0-2 |
| **Total** | | **~R$ 75-100/mês** |

> Para reduzir custos: use Free Tier do App Service (F1) para desenvolvimento.
