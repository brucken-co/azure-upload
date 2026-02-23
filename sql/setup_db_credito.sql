-- ============================================================
-- Brücken Upload Portal — Tabelas em db_credito
-- Executar em: srv-credito-analytics / db_credito
-- ============================================================

-- 1. Clientes autorizados a fazer upload
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'upload_clients')
BEGIN
    CREATE TABLE upload_clients (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        client_id       VARCHAR(50) NOT NULL UNIQUE,
        client_name     NVARCHAR(200) NOT NULL,
        token_hash      VARCHAR(64) NOT NULL,
        container_prefix VARCHAR(50) NOT NULL,
        is_active       BIT DEFAULT 1,
        max_file_size_mb INT DEFAULT 500,
        allowed_extensions VARCHAR(200) DEFAULT 'csv,xlsx,xls,json,txt,parquet',
        created_at      DATETIME2 DEFAULT GETUTCDATE(),
        updated_at      DATETIME2 DEFAULT GETUTCDATE()
    );
    PRINT '✓ Tabela upload_clients criada';
END
ELSE PRINT '→ upload_clients já existe';
GO

-- 2. Registro de cada arquivo enviado
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'upload_files')
BEGIN
    CREATE TABLE upload_files (
        id                  INT IDENTITY(1,1) PRIMARY KEY,
        client_id           VARCHAR(50) NOT NULL,
        original_filename   NVARCHAR(500) NOT NULL,
        blob_path           NVARCHAR(1000) NOT NULL,
        file_extension      VARCHAR(20) NOT NULL,
        file_size_bytes     BIGINT NOT NULL,
        upload_status       VARCHAR(20) DEFAULT 'uploaded',
        validation_errors   NVARCHAR(MAX) NULL,
        validation_warnings NVARCHAR(MAX) NULL,
        file_metadata       NVARCHAR(MAX) NULL,
        rows_loaded         INT NULL,
        uploaded_at         DATETIME2 DEFAULT GETUTCDATE(),
        validated_at        DATETIME2 NULL,
        loaded_at           DATETIME2 NULL,

        CONSTRAINT FK_upload_files_client
            FOREIGN KEY (client_id) REFERENCES upload_clients(client_id)
    );

    CREATE INDEX IX_upload_files_client ON upload_files(client_id);
    CREATE INDEX IX_upload_files_status ON upload_files(upload_status);
    CREATE INDEX IX_upload_files_date   ON upload_files(uploaded_at DESC);

    PRINT '✓ Tabela upload_files criada';
END
ELSE PRINT '→ upload_files já existe';
GO

-- 3. Dados importados dos clientes (staging para o motor de crédito)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_data')
BEGIN
    CREATE TABLE staging_data (
        id              BIGINT IDENTITY(1,1) PRIMARY KEY,
        upload_file_id  INT NOT NULL,
        client_id       VARCHAR(50) NOT NULL,
        row_number      INT NOT NULL,
        row_data        NVARCHAR(MAX) NOT NULL,
        loaded_at       DATETIME2 DEFAULT GETUTCDATE(),

        CONSTRAINT FK_staging_data_file
            FOREIGN KEY (upload_file_id) REFERENCES upload_files(id)
    );

    CREATE INDEX IX_staging_data_file   ON staging_data(upload_file_id);
    CREATE INDEX IX_staging_data_client ON staging_data(client_id);

    PRINT '✓ Tabela staging_data criada';
END
ELSE PRINT '→ staging_data já existe';
GO

-- 4. View para dashboard
CREATE OR ALTER VIEW vw_upload_dashboard AS
SELECT
    f.id AS upload_id,
    f.client_id,
    c.client_name,
    f.original_filename,
    f.file_extension,
    CAST(f.file_size_bytes / 1048576.0 AS DECIMAL(10,2)) AS file_size_mb,
    f.upload_status,
    f.rows_loaded,
    f.uploaded_at,
    f.validated_at,
    f.loaded_at,
    DATEDIFF(SECOND, f.uploaded_at, f.loaded_at) AS processing_seconds
FROM upload_files f
JOIN upload_clients c ON f.client_id = c.client_id;
GO

PRINT '✓ View vw_upload_dashboard criada';
GO

-- 5. Clientes demo
IF NOT EXISTS (SELECT 1 FROM upload_clients WHERE client_id = 'CLI-00123')
BEGIN
    INSERT INTO upload_clients (client_id, client_name, token_hash, container_prefix)
    VALUES (
        'CLI-00123',
        'Empresa Demo LTDA',
        'ef92b778bafe771e89245b89ecbc08a44a4e166c06659911881f383d4473e94f',
        'cli-00123'
    );
    PRINT '✓ Cliente demo CLI-00123 inserido';
END

IF NOT EXISTS (SELECT 1 FROM upload_clients WHERE client_id = 'CLI-00456')
BEGIN
    INSERT INTO upload_clients (client_id, client_name, token_hash, container_prefix)
    VALUES (
        'CLI-00456',
        'Financeira ABC S.A.',
        'c0e81794384491161f1777c232bc6bd9ec38f616560b120fda8e90f383853542',
        'cli-00456'
    );
    PRINT '✓ Cliente demo CLI-00456 inserido';
END
GO

PRINT '';
PRINT '============================================';
PRINT '  Setup concluído em db_credito!';
PRINT '  Tabelas: upload_clients, upload_files, staging_data';
PRINT '  View: vw_upload_dashboard';
PRINT '============================================';
