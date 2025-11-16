/*
================================================================================
SCRIPT: 00a_create_logging_utility.sql
PURPOSE: One-time setup to create a persistent logging table for ETL processes.
RUN AS:  DBA or Admin User
RUN ON:  Initial setup.
================================================================================
*/

SELECT 'Script 00a_create_logging_utility.sql started...' AS 'Admin_Status';
USE dw_bronze;

/*
================================================================================
Section 1: Create ETL Log Table
Purpose: This table will capture the status, error messages, and performance
         (duration) of all automated ETL jobs.
================================================================================
*/
SELECT 'Creating table etl_log...' AS 'Admin_Status';
DROP TABLE IF EXISTS etl_log;
CREATE TABLE IF NOT EXISTS etl_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    start_time DATETIME(6) NOT NULL,
    end_time DATETIME(6),
    duration_sec DECIMAL(10, 4),
    status ENUM('In Progress', 'Success', 'Error') NOT NULL,
    log_message TEXT,
    INDEX idx_process_name (process_name),
    INDEX idx_start_time (start_time)
);

SELECT 'Script 00a_create_logging_utility.sql finished.' AS 'Admin_Status';

-- End of script --