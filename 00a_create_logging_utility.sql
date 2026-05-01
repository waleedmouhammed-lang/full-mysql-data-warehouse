/*
================================================================================
SCRIPT: 00a_create_logging_utility.sql
PURPOSE: Creates SQL Server operational metadata tables in ops schema.
================================================================================
*/

USE DataWarehouse;
GO

IF OBJECT_ID(N'ops.job_runs', N'U') IS NULL
BEGIN
    CREATE TABLE ops.job_runs (
        job_run_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT pk_ops_job_runs PRIMARY KEY
            CONSTRAINT df_ops_job_runs_id DEFAULT NEWID(),
        pipeline_name NVARCHAR(128) NOT NULL,
        status VARCHAR(20) NOT NULL
            CONSTRAINT ck_ops_job_runs_status CHECK (status IN ('Running', 'Success', 'Failed')),
        started_at DATETIME2(6) NOT NULL
            CONSTRAINT df_ops_job_runs_started_at DEFAULT SYSUTCDATETIME(),
        ended_at DATETIME2(6) NULL,
        duration_sec DECIMAL(18, 4) NULL,
        message NVARCHAR(MAX) NULL
    );

    CREATE INDEX ix_ops_job_runs_started_at ON ops.job_runs(started_at);
    CREATE INDEX ix_ops_job_runs_status ON ops.job_runs(status);
END;
GO

IF OBJECT_ID(N'ops.task_runs', N'U') IS NULL
BEGIN
    CREATE TABLE ops.task_runs (
        task_run_id BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT pk_ops_task_runs PRIMARY KEY,
        job_run_id UNIQUEIDENTIFIER NOT NULL,
        task_name NVARCHAR(128) NOT NULL,
        status VARCHAR(20) NOT NULL
            CONSTRAINT ck_ops_task_runs_status CHECK (status IN ('Running', 'Success', 'Failed')),
        started_at DATETIME2(6) NOT NULL
            CONSTRAINT df_ops_task_runs_started_at DEFAULT SYSUTCDATETIME(),
        ended_at DATETIME2(6) NULL,
        duration_sec DECIMAL(18, 4) NULL,
        rows_read INT NULL,
        rows_inserted INT NULL,
        rows_updated INT NULL,
        rows_rejected INT NULL,
        message NVARCHAR(MAX) NULL,
        CONSTRAINT fk_ops_task_runs_job_runs
            FOREIGN KEY (job_run_id) REFERENCES ops.job_runs(job_run_id)
    );

    CREATE INDEX ix_ops_task_runs_job_run_id ON ops.task_runs(job_run_id);
END;
GO

IF OBJECT_ID(N'ops.source_file_audit', N'U') IS NULL
BEGIN
    CREATE TABLE ops.source_file_audit (
        source_file_audit_id BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT pk_ops_source_file_audit PRIMARY KEY,
        job_run_id UNIQUEIDENTIFIER NOT NULL,
        source_name NVARCHAR(128) NOT NULL,
        source_file NVARCHAR(4000) NOT NULL,
        file_size_bytes BIGINT NULL,
        row_count INT NULL,
        file_modified_at DATETIME2(6) NULL,
        loaded_at DATETIME2(6) NOT NULL
            CONSTRAINT df_ops_source_file_audit_loaded_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT fk_ops_source_file_audit_job_runs
            FOREIGN KEY (job_run_id) REFERENCES ops.job_runs(job_run_id)
    );
END;
GO

IF OBJECT_ID(N'ops.data_quality_results', N'U') IS NULL
BEGIN
    CREATE TABLE ops.data_quality_results (
        data_quality_result_id BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT pk_ops_data_quality_results PRIMARY KEY,
        job_run_id UNIQUEIDENTIFIER NULL,
        model_name NVARCHAR(256) NOT NULL,
        test_name NVARCHAR(256) NOT NULL,
        status VARCHAR(20) NOT NULL
            CONSTRAINT ck_ops_data_quality_results_status CHECK (status IN ('Pass', 'Warn', 'Fail')),
        failed_row_count INT NULL,
        checked_at DATETIME2(6) NOT NULL
            CONSTRAINT df_ops_data_quality_results_checked_at DEFAULT SYSUTCDATETIME(),
        details NVARCHAR(MAX) NULL
    );
END;
GO

IF OBJECT_ID(N'ops.table_row_counts', N'U') IS NULL
BEGIN
    CREATE TABLE ops.table_row_counts (
        table_row_count_id BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT pk_ops_table_row_counts PRIMARY KEY,
        job_run_id UNIQUEIDENTIFIER NULL,
        schema_name SYSNAME NOT NULL,
        table_name SYSNAME NOT NULL,
        row_count BIGINT NOT NULL,
        captured_at DATETIME2(6) NOT NULL
            CONSTRAINT df_ops_table_row_counts_captured_at DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF OBJECT_ID(N'ops.alerts', N'U') IS NULL
BEGIN
    CREATE TABLE ops.alerts (
        alert_id BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT pk_ops_alerts PRIMARY KEY,
        job_run_id UNIQUEIDENTIFIER NULL,
        severity VARCHAR(20) NOT NULL
            CONSTRAINT ck_ops_alerts_severity CHECK (severity IN ('Info', 'Warning', 'Critical')),
        channel VARCHAR(50) NULL,
        subject NVARCHAR(256) NOT NULL,
        message NVARCHAR(MAX) NULL,
        created_at DATETIME2(6) NOT NULL
            CONSTRAINT df_ops_alerts_created_at DEFAULT SYSUTCDATETIME(),
        sent_at DATETIME2(6) NULL
    );
END;
GO
