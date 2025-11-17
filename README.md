## **Project Documentation: Python-Orchestrated Bronze Layer ETL Pipeline**

---

### **1. Executive Summary**

This project implements a professional, production-grade ETL (Extract, Transform, Load) pipeline for ingesting data into a MySQL-based data warehouse. The architecture follows a modern, **Python-orchestrated model** designed to be resilient, maintainable, observable, and secure.

The primary function of this pipeline is to perform a robust, **incremental (UPSERT) load** of data from multiple CSV source files into a **Bronze "Medallion" layer**. It intelligently handles new and updated records by leveraging a staging table workflow and MySQL's `ON DUPLICATE KEY UPDATE` feature. All orchestration, error handling, and logging are centralized within a single Python script, deliberately moving this logic out of the database for flexibility and scalability.

### **2. Architectural Philosophy & Design Principles**

The system is built on four key principles:

1.  **Resilience:** The pipeline must not fail due to "dirty" source data. This is achieved by using `VARCHAR(255)` for all columns in the Bronze layer, ensuring that any data type mismatch (e.g., 'N/A' in a number column) is captured as a string without failing the load. Data cleaning is deliberately deferred to the Silver layer.
2.  **Maintainability:** All logic is managed in version-controlled files (SQL and Python), not hidden in complex stored procedures.
3.  **Observability:** The pipeline features a dual-logging system:
    * **Database Log (`etl_log`):** A persistent, queryable table that captures the status, duration, and error messages of every run. This turns logs into data, enabling monitoring and performance analysis.
    * **File Log (`bronze_load.log`):** A standard text file for real-time debugging and detailed tracebacks, configured by the Python `logging` module.
4.  **Security:** Credentials and file paths are externalized from the code using a `.env` file. This file is listed in `.gitignore` to prevent leaking secrets to version control.

### **3. Core Architecture & File Structure**

The system is split into two phases: a one-time setup and the daily orchestration.

#### **Phase 1: One-Time Setup Files**
These scripts are run once by an administrator or engineer to build the warehouse structure.

* `00_create_warehouse_schema.sql`: **(DBA Task)** Creates the three core Medallion databases: `dw_bronze`, `dw_silver`, and `dw_gold`.
* `00a_create_logging_utility.sql`: **(DBA Task)** Creates the persistent `etl_log` table in the `dw_bronze` database for centralized monitoring.
* `01_create_bronze_tables.sql`: **(Engineer Task)** Defines the schema for the final Bronze tables (e.g., `crm_cust_info`, `erp_loc_a101`). Critically, it adds **`UNIQUE KEY` constraints** to the business keys (e.g., `idx_cst_id (cst_id)`). These keys are essential for the incremental UPSERT logic.
* `02_create_staging_tables.sql`: **(Engineer Task)** Defines the schemas for the transient staging tables (e.g., `stg_crm_cust_info`). These tables are created to be **structurally identical** to the Bronze tables but **deliberately lack unique keys**. This allows raw data, including duplicates, to be loaded rapidly without constraints before the merge logic is applied.

#### **Phase 2: Daily Orchestration Files**
These files are used in the automated, repeatable ETL process.

* `run_bronze_load.py`: **(The "Brain")** The Python script that orchestrates the entire process. It connects to the database, reads the configuration, iterates through tables, and executes the Staging-to-Bronze load workflow, complete with robust logging and error handling.
* `.env`: **(Secret File)** Not provided, but referenced. This file stores credentials (host, user, password) and the file paths to the source CSV data. It is read by the Python script.
* `requirements.txt`: Defines the necessary Python libraries: `mysql-connector-python` (to talk to MySQL) and `python-dotenv` (to read the `.env` file).
* `.gitignore`: A configuration file for Git (version control) that explicitly ignores sensitive files like `.env`, log files (`*.log`), and Python cache directories.

---

### **4. Detailed Project Workflow**

The project operates in two distinct phases:

#### **Phase 1: One-Time Setup (Manual)**
An administrator runs the SQL scripts in order to prepare the data warehouse:
1.  **`00_create_warehouse_schema.sql`** is run to create the `dw_bronze`, `dw_silver`, and `dw_gold` databases.
2.  **`00a_create_logging_utility.sql`** is run to create the `etl_log` table within `dw_bronze`.
3.  **`01_create_bronze_tables.sql`** is run to create the final, constrained Bronze tables.
4.  **`02_create_staging_tables.sql`** is run to create the transient, unconstrained staging tables.

#### **Phase 2: Daily ETL Workflow (Automated via `run_bronze_load.py`)**
This is the automated process orchestrated by the Python script.

1.  **Initialization:** The script starts. The `setup_logging()` function configures logging to both the console and the `bronze_load.log` file. The `read_config()` function securely loads database credentials and file paths from the `.env` file.
2.  **Connect & Log Start:** The script connects to the MySQL database, crucially setting `allow_local_infile=True`. It immediately calls `log_etl_start()`, which inserts a new row into the `etl_log` table with the status "In Progress" and returns the `log_id` for this run.
3.  **Begin Table Loop:** The script iterates through a list of table configurations (e.g., `crm_cust_info`, `crm_prd_info`, etc.) defined in the `tables_to_load` dictionary list.
4.  **For *each* table, it performs the following workflow:**
    a.  **Truncate Staging Table:** It executes a `TRUNCATE TABLE stg_{table_name}` command. This is a high-performance operation to clear the staging table from the previous run.
    b.  **Load to Staging Table:** It executes a `LOAD DATA LOCAL INFILE` command. This streams the data from the source CSV file *directly* into the unconstrained staging table (`stg_{table_name}`). Because the staging table has no unique keys, this step is fast and will not fail on duplicate data.
    c.  **UPSERT to Bronze Table:** It dynamically builds and executes an `INSERT INTO ... ON DUPLICATE KEY UPDATE` query. This query selects all data *from* the staging table and merges it *into* the final Bronze table (`{table_name}`).
        * If a row's unique key is new, it's **inserted**.
        * If a row's unique key already exists, its columns are **updated** with the new values from the file.
    d.  **Commit:** After the UPSERT is complete for a single table, `connection.commit()` is called. This is a "Partial Success" model, meaning if the script fails on the 5th table, the first 4 are successfully saved.
5.  **Log End (Success):** After the loop finishes without errors, `log_etl_end()` is called. It updates the corresponding row in `etl_log` to "Success" and records the total `duration_sec`.
6.  **Error Handling (Catch):** The entire process is wrapped in a `try...except` block. If *any* error occurs (a database error or a Python error), the `except` block catches it. It logs the failure and calls `log_etl_end()` to update the `etl_log` table with a "Error" status and the specific error message. The script then exits with an error code.
7.  **Cleanup (Finally):** A `finally` block ensures that, regardless of success or failure, the database connection is always closed cleanly.

---

### **5. Key Architectural Decisions & Justifications**

* **Why Python Orchestration (vs. a Stored Procedure)?**
    This is the core design decision. A pure SQL stored procedure would be blocked by the `secure_file_priv` server setting, which disables `LOAD DATA INFILE` for security. The Python script acts as a **client**, allowing it to use `LOAD DATA **LOCAL** INFILE`, which bypasses this restriction entirely. Python also provides superior error handling (`try...except...finally`) and more flexible logging.

* **Why use Staging Tables?**
    The staging tables are the key to the incremental UPSERT strategy. They act as a transient buffer. This separation of logic is critical:
    1.  **Fast Ingestion:** Loading the raw file into the *unconstrained* staging table is extremely fast and resilient.
    2.  **Clean Merge:** The `INSERT ... ON DUPLICATE KEY UPDATE` command, which performs the actual merge, can then operate cleanly between two database tables (staging and bronze) without the overhead of file parsing. This pattern is far more robust and performant than a row-by-row Python loop.

* **Why `VARCHAR(255)` for all Bronze columns?**
    **Resilience**. The Bronze layer's job is to capture raw data, "warts and all". If a source file contains bad data (e.g., "S043697" in a cost column), a strictly-typed table (e.g., `DECIMAL(10,2)`) would fail the entire load. Using `VARCHAR(255)` ensures the "dirty" data is captured as a string, and the load *always succeeds*. Data typing and cleaning are deferred to the Silver layer.

* **Why a dedicated `etl_log` table?**
    **Observability**. Standard file logs or `SELECT 'message'` statements are "dumb" and cannot be easily queried for trends. By writing logs to a structured SQL table, we turn logs into data. This allows for powerful queries like:
    * `SELECT * FROM etl_log WHERE status = 'Error' ORDER BY start_time DESC;`
    * `SELECT process_name, AVG(duration_sec) FROM etl_log GROUP BY process_name;`
    This enables proactive monitoring, dashboarding, and performance bottleneck analysis.

* **Why use `.env` files?**
    **Security & Portability**. Hard-coding credentials (e.g., `password='root'`) in the script is a massive security risk, especially if the code is shared or committed to Git. The `.env` file (which is ignored by Git) allows credentials to be stored securely on the host machine. This also allows for different `.env` files for development, testing, and production environments.
