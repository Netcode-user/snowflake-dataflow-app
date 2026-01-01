CREATE SCHEMA IF NOT EXISTS app_schema;
USE SCHEMA app_schema;

CREATE OR REPLACE TABLE data_profile_results (
    profile_id STRING DEFAULT UUID_STRING(),
    table_name STRING NOT NULL,
    column_name STRING NOT NULL,
    data_type STRING,
    row_count NUMBER,
    null_count NUMBER,
    null_percentage FLOAT,
    distinct_count NUMBER,
    distinct_percentage FLOAT,
    min_value VARIANT,
    max_value VARIANT,
    avg_value FLOAT,
    sample_values ARRAY,
    profiled_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (profile_id)
);

CREATE OR REPLACE TABLE quality_check_configs (
    check_id STRING DEFAULT UUID_STRING(),
    check_name STRING NOT NULL,
    table_name STRING NOT NULL,
    column_name STRING,
    check_type STRING NOT NULL,
    check_parameters VARIANT,
    severity STRING DEFAULT 'WARNING',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (check_id)
);

CREATE OR REPLACE TABLE quality_check_results (
    result_id STRING DEFAULT UUID_STRING(),
    check_id STRING NOT NULL,
    execution_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    status STRING,
    records_checked NUMBER,
    records_failed NUMBER,
    failure_rate FLOAT,
    details VARIANT,
    PRIMARY KEY (result_id)
);

CREATE OR REPLACE TABLE transformation_jobs (
    job_id STRING DEFAULT UUID_STRING(),
    job_name STRING NOT NULL,
    source_table STRING NOT NULL,
    target_table STRING NOT NULL,
    transformation_type STRING NOT NULL,
    transformation_config VARIANT,
    schedule STRING,
    is_active BOOLEAN DEFAULT TRUE,
    last_run TIMESTAMP_NTZ,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (job_id)
);

CREATE OR REPLACE TABLE job_execution_history (
    execution_id STRING DEFAULT UUID_STRING(),
    job_id STRING NOT NULL,
    started_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    completed_at TIMESTAMP_NTZ,
    status STRING,
    rows_processed NUMBER,
    rows_affected NUMBER,
    error_message STRING,
    execution_time_seconds FLOAT,
    PRIMARY KEY (execution_id)
);

CREATE OR REPLACE PROCEDURE profile_table(
    target_table STRING,
    sample_size NUMBER DEFAULT 100
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result_message STRING;
    col_cursor CURSOR FOR 
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = :target_table;
BEGIN
    DELETE FROM data_profile_results WHERE table_name = :target_table;
    FOR col IN col_cursor DO
        LET col_name := col.column_name;
        LET col_type := col.data_type;
        EXECUTE IMMEDIATE '
            INSERT INTO data_profile_results (
                table_name, column_name, data_type, row_count, 
                null_count, null_percentage, distinct_count, distinct_percentage,
                min_value, max_value, avg_value, sample_values
            )
            SELECT 
                ''' || :target_table || ''',
                ''' || :col_name || ''',
                ''' || :col_type || ''',
                COUNT(*) as row_count,
                COUNT(*) - COUNT(' || :col_name || ') as null_count,
                ROUND((COUNT(*) - COUNT(' || :col_name || ')) * 100.0 / NULLIF(COUNT(*), 0), 2) as null_percentage,
                COUNT(DISTINCT ' || :col_name || ') as distinct_count,
                ROUND(COUNT(DISTINCT ' || :col_name || ') * 100.0 / NULLIF(COUNT(' || :col_name || '), 0), 2) as distinct_percentage,
                TO_VARIANT(MIN(' || :col_name || ')) as min_value,
                TO_VARIANT(MAX(' || :col_name || ')) as max_value,
                TRY_CAST(AVG(TRY_CAST(' || :col_name || ' AS FLOAT)) AS FLOAT) as avg_value,
                ARRAY_AGG(TO_VARIANT(' || :col_name || ')) WITHIN GROUP (ORDER BY RANDOM()) LIMIT ' || :sample_size || ' as sample_values
            FROM ' || :target_table;
    END FOR;
    result_message := 'Successfully profiled table: ' || :target_table;
    RETURN result_message;
END;
$$;

CREATE OR REPLACE PROCEDURE run_quality_checks(
    target_table STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    check_cursor CURSOR FOR 
        SELECT check_id, check_name, table_name, column_name, check_type, check_parameters, severity
        FROM quality_check_configs 
        WHERE table_name = :target_table AND is_active = TRUE;
    total_checks NUMBER := 0;
BEGIN
    FOR check IN check_cursor DO
        LET check_id := check.check_id;
        LET check_type := check.check_type;
        LET column_name := check.column_name;
        LET check_params := check.check_parameters;
        IF (check_type = 'NULL_CHECK') THEN
            EXECUTE IMMEDIATE '
                INSERT INTO quality_check_results (check_id, status, records_checked, records_failed, failure_rate, details)
                SELECT 
                    ''' || :check_id || ''',
                    CASE WHEN null_count = 0 THEN ''PASSED'' ELSE ''FAILED'' END,
                    total_count,
                    null_count,
                    ROUND(null_count * 100.0 / NULLIF(total_count, 0), 2),
                    OBJECT_CONSTRUCT(''message'', ''Found '' || null_count || '' null values'')
                FROM (
                    SELECT 
                        COUNT(*) as total_count,
                        COUNT(*) - COUNT(' || :column_name || ') as null_count
                    FROM ' || :target_table || '
                )';
        ELSEIF (check_type = 'DUPLICATE_CHECK') THEN
            EXECUTE IMMEDIATE '
                INSERT INTO quality_check_results (check_id, status, records_checked, records_failed, failure_rate, details)
                SELECT 
                    ''' || :check_id || ''',
                    CASE WHEN duplicate_count = 0 THEN ''PASSED'' ELSE ''FAILED'' END,
                    total_count,
                    duplicate_count,
                    ROUND(duplicate_count * 100.0 / NULLIF(total_count, 0), 2),
                    OBJECT_CONSTRUCT(''message'', ''Found '' || duplicate_count || '' duplicate values'')
                FROM (
                    SELECT 
                        COUNT(*) as total_count,
                        COUNT(*) - COUNT(DISTINCT ' || :column_name || ') as duplicate_count
                    FROM ' || :target_table || '
                )';
        END IF;
        total_checks := total_checks + 1;
    END FOR;
    RETURN 'Completed ' || total_checks || ' quality checks on ' || :target_table;
END;
$$;

CREATE OR REPLACE PROCEDURE deduplicate_table(
    source_table STRING,
    target_table STRING,
    key_columns ARRAY
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    key_cols_str STRING;
    rows_before NUMBER;
    rows_after NUMBER;
BEGIN
    SELECT ARRAY_TO_STRING(:key_columns, ', ') INTO :key_cols_str;
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || :source_table INTO :rows_before;
    EXECUTE IMMEDIATE '
        CREATE OR REPLACE TABLE ' || :target_table || ' AS
        SELECT * FROM (
            SELECT *, 
                   ROW_NUMBER() OVER (PARTITION BY ' || :key_cols_str || ' ORDER BY (SELECT NULL)) as rn
            FROM ' || :source_table || '
        )
        WHERE rn = 1';
    EXECUTE IMMEDIATE 'ALTER TABLE ' || :target_table || ' DROP COLUMN rn';
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || :target_table INTO :rows_after;
    RETURN 'Deduplication complete. Rows before: ' || :rows_before || ', Rows after: ' || :rows_after || ', Duplicates removed: ' || (:rows_before - :rows_after);
END;
$$;

CREATE OR REPLACE PROCEDURE clean_null_values(
    source_table STRING,
    target_table STRING,
    strategy STRING,
    columns_to_clean ARRAY DEFAULT NULL
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    col_cursor CURSOR FOR 
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = :source_table;
    select_clause STRING := '';
    first_col BOOLEAN := TRUE;
BEGIN
    IF (strategy = 'DROP') THEN
        IF (columns_to_clean IS NULL) THEN
            EXECUTE IMMEDIATE '
                CREATE OR REPLACE TABLE ' || :target_table || ' AS
                SELECT * FROM ' || :source_table || '
                WHERE NOT EXISTS (
                    SELECT 1 FROM information_schema.columns c
                    WHERE c.table_name = ''' || :source_table || '''
                    AND COLUMN_NAME IS NULL
                )';
        ELSE
            LET where_clause STRING := '';
            FOR i IN 0 TO ARRAY_SIZE(:columns_to_clean) - 1 DO
                IF (i > 0) THEN
                    where_clause := where_clause || ' AND ';
                END IF;
                where_clause := where_clause || columns_to_clean[i] || ' IS NOT NULL';
            END FOR;
            EXECUTE IMMEDIATE '
                CREATE OR REPLACE TABLE ' || :target_table || ' AS
                SELECT * FROM ' || :source_table || '
                WHERE ' || :where_clause;
        END IF;
    ELSEIF (strategy = 'FILL_ZERO') THEN
        FOR col IN col_cursor DO
            IF (NOT first_col) THEN
                select_clause := select_clause || ', ';
            END IF;
            IF (col.data_type IN ('NUMBER', 'FLOAT', 'INTEGER')) THEN
                select_clause := select_clause || 'COALESCE(' || col.column_name || ', 0) AS ' || col.column_name;
            ELSE
                select_clause := select_clause || col.column_name;
            END IF;
            first_col := FALSE;
        END FOR;
        EXECUTE IMMEDIATE '
            CREATE OR REPLACE TABLE ' || :target_table || ' AS
            SELECT ' || :select_clause || '
            FROM ' || :source_table;
    END IF;
    RETURN 'Null value cleaning complete using strategy: ' || :strategy;
END;
$$;

CREATE OR REPLACE PROCEDURE standardize_text_column(
    source_table STRING,
    target_table STRING,
    column_name STRING,
    operation STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    transformation_expr STRING;
BEGIN
    CASE (operation)
        WHEN 'UPPERCASE' THEN
            transformation_expr := 'UPPER(' || :column_name || ')';
        WHEN 'LOWERCASE' THEN
            transformation_expr := 'LOWER(' || :column_name || ')';
        WHEN 'TRIM' THEN
            transformation_expr := 'TRIM(' || :column_name || ')';
        WHEN 'REMOVE_SPECIAL_CHARS' THEN
            transformation_expr := 'REGEXP_REPLACE(' || :column_name || ', ''[^a-zA-Z0-9 ]'', '''')';
        ELSE
            RETURN 'Invalid operation: ' || :operation;
    END CASE;
    EXECUTE IMMEDIATE '
        CREATE OR REPLACE TABLE ' || :target_table || ' AS
        SELECT 
            ' || :transformation_expr || ' AS ' || :column_name || ',
            * EXCLUDE ' || :column_name || '
        FROM ' || :source_table;
    RETURN 'Text standardization complete: ' || :operation || ' applied to ' || :column_name;
END;
$$;

CREATE OR REPLACE PROCEDURE execute_transformation_job(
    job_id_param STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    job_record VARIANT;
    execution_id STRING;
    start_time TIMESTAMP_NTZ;
    end_time TIMESTAMP_NTZ;
    job_status STRING;
    error_msg STRING;
BEGIN
    start_time := CURRENT_TIMESTAMP();
    execution_id := UUID_STRING();
    SELECT OBJECT_CONSTRUCT(*) INTO job_record FROM transformation_jobs WHERE job_id = :job_id_param;
    INSERT INTO job_execution_history (execution_id, job_id, status)
    VALUES (:execution_id, :job_id_param, 'RUNNING');
    BEGIN
        CASE (job_record:transformation_type::STRING)
            WHEN 'DEDUPLICATE' THEN
                CALL deduplicate_table(
                    job_record:source_table::STRING,
                    job_record:target_table::STRING,
                    job_record:transformation_config:key_columns::ARRAY
                );
            WHEN 'CLEAN_NULLS' THEN
                CALL clean_null_values(
                    job_record:source_table::STRING,
                    job_record:target_table::STRING,
                    job_record:transformation_config:strategy::STRING,
                    job_record:transformation_config:columns::ARRAY
                );
            WHEN 'STANDARDIZE' THEN
                CALL standardize_text_column(
                    job_record:source_table::STRING,
                    job_record:target_table::STRING,
                    job_record:transformation_config:column_name::STRING,
                    job_record:transformation_config:operation::STRING
                );
        END CASE;
        job_status := 'SUCCESS';
        error_msg := NULL;
    EXCEPTION
        WHEN OTHER THEN
            job_status := 'FAILED';
            error_msg := SQLERRM;
    END;
    end_time := CURRENT_TIMESTAMP();
    UPDATE job_execution_history
    SET completed_at = :end_time,
        status = :job_status,
        error_message = :error_msg,
        execution_time_seconds = DATEDIFF('second', :start_time, :end_time)
    WHERE execution_id = :execution_id;
    UPDATE transformation_jobs
    SET last_run = :end_time
    WHERE job_id = :job_id_param;
    RETURN 'Job execution complete. Status: ' || :job_status;
END;
$$;

GRANT USAGE ON SCHEMA app_schema TO APPLICATION ROLE app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA app_schema TO APPLICATION ROLE app_user;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA app_schema TO APPLICATION ROLE app_user;

SELECT 'DataFlow Pro setup complete!' AS status;
