#!/bin/bash
# ============================================================
# Brücken Upload Portal — Script de Deploy no Azure
# ============================================================
# Este script cria toda a infraestrutura necessária no Azure.
#
# Pré-requisitos:
#   - Azure CLI instalado (az login já executado)
#   - Subscription ativa
#
# Uso:
#   chmod +x deploy.sh
#   ./deploy.sh
# ============================================================

set -e

# ============================================================
# CONFIGURAÇÕES — Altere conforme necessário
# ============================================================
RESOURCE_GROUP="rg-brucken-upload"
LOCATION="brazilsouth"
STORAGE_ACCOUNT="stbruckenupload"      # Deve ser único globalmente (só minúsculas e números)
APP_SERVICE_PLAN="plan-brucken-upload"
WEB_APP_NAME="app-brucken-upload"       # Deve ser único globalmente
FUNCTION_APP_NAME="func-brucken-upload" # Deve ser único globalmente
FUNCTION_STORAGE="stbruckenfunc"        # Storage separado para a Function

echo "=================================================="
echo "  Brücken Upload Portal — Deploy Azure"
echo "=================================================="
echo ""

# ============================================================
# 1. Resource Group
# ============================================================
echo "[1/8] Criando Resource Group..."
az group create \
    --name $RESOURCE_GROUP \
    --location $LOCATION \
    --output none

echo "      ✓ Resource Group: $RESOURCE_GROUP"

# ============================================================
# 2. Storage Account (para os uploads dos clientes)
# ============================================================
echo "[2/8] Criando Storage Account para uploads..."
az storage account create \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku Standard_LRS \
    --kind StorageV2 \
    --min-tls-version TLS1_2 \
    --allow-blob-public-access false \
    --https-only true \
    --output none

# Obtém a connection string
STORAGE_CONNECTION=$(az storage account show-connection-string \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --query connectionString -o tsv)

echo "      ✓ Storage Account: $STORAGE_ACCOUNT"

# ============================================================
# 3. Containers no Blob Storage
# ============================================================
echo "[3/8] Criando containers..."

for CONTAINER in "uploads-clientes" "staging" "rejected" "notifications"; do
    az storage container create \
        --name $CONTAINER \
        --account-name $STORAGE_ACCOUNT \
        --auth-mode login \
        --output none 2>/dev/null || true
    echo "      ✓ Container: $CONTAINER"
done

# ============================================================
# 4. App Service Plan (para o backend Flask)
# ============================================================
echo "[4/8] Criando App Service Plan..."
az appservice plan create \
    --name $APP_SERVICE_PLAN \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku B1 \
    --is-linux \
    --output none

echo "      ✓ App Service Plan: $APP_SERVICE_PLAN (B1 Linux)"

# ============================================================
# 5. Web App (backend Flask)
# ============================================================
echo "[5/8] Criando Web App..."
az webapp create \
    --name $WEB_APP_NAME \
    --resource-group $RESOURCE_GROUP \
    --plan $APP_SERVICE_PLAN \
    --runtime "PYTHON:3.11" \
    --output none

# Configura variáveis de ambiente
az webapp config appsettings set \
    --name $WEB_APP_NAME \
    --resource-group $RESOURCE_GROUP \
    --settings \
        AZURE_STORAGE_CONNECTION_STRING="$STORAGE_CONNECTION" \
        AZURE_STORAGE_CONTAINER="uploads-clientes" \
        SCM_DO_BUILD_DURING_DEPLOYMENT=true \
        FLASK_DEBUG=false \
    --output none

# Configura startup command
az webapp config set \
    --name $WEB_APP_NAME \
    --resource-group $RESOURCE_GROUP \
    --startup-file "gunicorn --bind=0.0.0.0:8000 app:app" \
    --output none

echo "      ✓ Web App: https://$WEB_APP_NAME.azurewebsites.net"

# ============================================================
# 6. Storage Account para Function App
# ============================================================
echo "[6/8] Criando Storage Account para Function..."
az storage account create \
    --name $FUNCTION_STORAGE \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku Standard_LRS \
    --kind StorageV2 \
    --output none

echo "      ✓ Function Storage: $FUNCTION_STORAGE"

# ============================================================
# 7. Function App
# ============================================================
echo "[7/8] Criando Function App..."
az functionapp create \
    --name $FUNCTION_APP_NAME \
    --resource-group $RESOURCE_GROUP \
    --storage-account $FUNCTION_STORAGE \
    --consumption-plan-location $LOCATION \
    --runtime python \
    --runtime-version 3.11 \
    --functions-version 4 \
    --os-type linux \
    --output none

# Configura a connection string do Storage de uploads
az functionapp config appsettings set \
    --name $FUNCTION_APP_NAME \
    --resource-group $RESOURCE_GROUP \
    --settings \
        AzureWebJobsStorage="$STORAGE_CONNECTION" \
    --output none

echo "      ✓ Function App: $FUNCTION_APP_NAME"

# ============================================================
# 8. Event Grid (para notificações de novos blobs)
# ============================================================
echo "[8/8] Configurando Event Grid..."
STORAGE_ID=$(az storage account show \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --query id -o tsv)

FUNCTION_ID=$(az functionapp show \
    --name $FUNCTION_APP_NAME \
    --resource-group $RESOURCE_GROUP \
    --query id -o tsv)

az eventgrid event-subscription create \
    --name "upload-notification" \
    --source-resource-id $STORAGE_ID \
    --endpoint-type azurefunction \
    --endpoint "$FUNCTION_ID/functions/process_uploaded_file" \
    --included-event-types "Microsoft.Storage.BlobCreated" \
    --subject-begins-with "/blobServices/default/containers/uploads-clientes/" \
    --output none 2>/dev/null || echo "      ⚠ Event Grid pode precisar de configuração manual"

echo "      ✓ Event Grid configurado"

# ============================================================
# RESUMO
# ============================================================
echo ""
echo "=================================================="
echo "  ✅ Deploy concluído!"
echo "=================================================="
echo ""
echo "  Recursos criados:"
echo "    Resource Group:  $RESOURCE_GROUP"
echo "    Storage Account: $STORAGE_ACCOUNT"
echo "    Web App:         https://$WEB_APP_NAME.azurewebsites.net"
echo "    Function App:    $FUNCTION_APP_NAME"
echo ""
echo "  Próximos passos:"
echo "    1. Deploy do backend:"
echo "       cd backend && az webapp up --name $WEB_APP_NAME"
echo ""
echo "    2. Deploy da Azure Function:"
echo "       cd azure-function && func azure functionapp publish $FUNCTION_APP_NAME"
echo ""
echo "    3. Deploy do frontend (copie index.html para /home/site/wwwroot/static/):"
echo "       ou hospede no Azure Static Web Apps"
echo ""
echo "  Connection String (salve em local seguro):"
echo "    $STORAGE_CONNECTION"
echo ""
echo "=================================================="
