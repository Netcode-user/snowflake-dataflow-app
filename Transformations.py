import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import json

st.set_page_config(page_title="Transformations", page_icon="🔄", layout="wide")

session = get_active_session()

st.title("🔄 Data Transformations")
st.markdown("Apply common data transformations to clean and standardize your data")
st.markdown("---")

tab1, tab2, tab3 = st.tabs(["🔧 Quick Transform", "📋 Transformation History", "💡 Examples"])

with tab1:
    st.markdown("### Apply Data Transformation")
    transformation_type = st.selectbox(
        "Transformation Type",
        ["Deduplicate Records", "Clean Null Values", "Standardize Text", "Custom SQL"]
    )
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Source Configuration")
        try:
            databases = session.sql("SHOW DATABASES").collect()
            db_list = [row['name'] for row in databases]
            source_db = st.selectbox("Source Database", db_list, key="source_db")
            if source_db:
                schemas = session.sql(f"SHOW SCHEMAS IN DATABASE {source_db}").collect()
                schema_list = [row['name'] for row in schemas]
                source_schema = st.selectbox("Source Schema", schema_list, key="source_schema")
                if source_schema:
                    tables = session.sql(f"SHOW TABLES IN SCHEMA {source_db}.{source_schema}").collect()
                    table_list = [row['name'] for row in tables]
                    source_table = st.selectbox("Source Table", table_list, key="source_table")
                    source_full_name = f"{source_db}.{source_schema}.{source_table}"
        except Exception as e:
            st.error(f"Error loading source objects: {str(e)}")
            source_full_name = ""

    with col2:
        st.markdown("#### Target Configuration")
        target_table_name = st.text_input("Target Table Name", placeholder="e.g., cleaned_customers")
        if source_db and source_schema and target_table_name:
            target_full_name = f"{source_db}.{source_schema}.{target_table_name}"
            st.info(f"Will create: `{target_full_name}`")
        else:
            target_full_name = ""

    if transformation_type == "Deduplicate Records":
        st.markdown("### 🔍 Deduplication Configuration")
        if source_full_name:
            try:
                columns = session.sql(f"SHOW COLUMNS IN TABLE {source_full_name}").collect()
                column_list = [row['column_name'] for row in columns]
                key_columns = st.multiselect("Select Key Columns", column_list)
                st.info(f"💡 Rows with identical values in {', '.join(key_columns) if key_columns else 'selected columns'} will be considered duplicates")
                if st.button("🔄 Run Deduplication", type="primary", use_container_width=True):
                    if key_columns and target_full_name:
                        with st.spinner("Deduplicating data..."):
                            try:
                                key_cols_array = "['" + "','".join(key_columns) + "']"
                                result = session.call("app_schema.deduplicate_table", source_full_name, target_full_name, key_cols_array)
                                st.success(result)
                                st.balloons()
                            except Exception as e:
                                st.error(f"Error: {str(e)}")
                    else:
                        st.warning("Please select key columns and specify target table")
            except Exception as e:
                st.error(f"Error loading columns: {str(e)}")

    elif transformation_type == "Clean Null Values":
        st.markdown("### 🧹 Null Value Cleaning Configuration")
        strategy = st.selectbox("Cleaning Strategy", ["DROP - Remove rows with null values", "FILL_ZERO - Replace nulls with 0 (numeric columns)", "FILL_MEAN - Replace with column mean (numeric columns)", "FILL_MODE - Replace with most common value"])
        strategy_code = strategy.split(" - ")[0]
        if source_full_name:
            try:
                columns = session.sql(f"SHOW COLUMNS IN TABLE {source_full_name}").collect()
                column_list = [row['column_name'] for row in columns]
                apply_to_all = st.checkbox("Apply to all columns", value=True)
                if not apply_to_all:
                    columns_to_clean = st.multiselect("Select Columns to Clean", column_list)
                else:
                    columns_to_clean = None
                if st.button("🧹 Clean Null Values", type="primary", use_container_width=True):
                    if target_full_name:
                        with st.spinner("Cleaning null values..."):
                            try:
                                cols_array = None if columns_to_clean is None else "['" + "','".join(columns_to_clean) + "']"
                                result = session.call("app_schema.clean_null_values", source_full_name, target_full_name, strategy_code, cols_array)
                                st.success(result)
                                st.balloons()
                            except Exception as e:
                                st.error(f"Error: {str(e)}")
                    else:
                        st.warning("Please specify target table")
            except Exception as e:
                st.error(f"Error loading columns: {str(e)}")

    elif transformation_type == "Standardize Text":
        st.markdown("### 📝 Text Standardization Configuration")
        if source_full_name:
            try:
                columns = session.sql(f"SHOW COLUMNS IN TABLE {source_full_name}").collect()
                text_columns = [row['column_name'] for row in columns if 'VARCHAR' in row['type'] or 'TEXT' in row['type'] or 'STRING' in row['type']]
                if text_columns:
                    column_to_standardize = st.selectbox("Select Text Column", text_columns)
                    operation = st.selectbox("Standardization Operation", ["UPPERCASE - Convert to uppercase", "LOWERCASE - Convert to lowercase", "TRIM - Remove leading/trailing spaces", "REMOVE_SPECIAL_CHARS - Remove special characters"])
                    operation_code = operation.split(" - ")[0]
                    if st.button("📝 Standardize Text", type="primary", use_container_width=True):
                        if target_full_name:
                            with st.spinner("Standardizing text..."):
                                try:
                                    result = session.call("app_schema.standardize_text_column", source_full_name, target_full_name, column_to_standardize, operation_code)
                                    st.success(result)
                                    st.balloons()
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                        else:
                            st.warning("Please specify target table")
                else:
                    st.warning("No text columns found in the selected table")
            except Exception as e:
                st.error(f"Error loading columns: {str(e)}")

    elif transformation_type == "Custom SQL":
        st.markdown("### 💻 Custom SQL Transformation")
        custom_sql = st.text_area("SQL Query", placeholder="""SELECT 
    col1,
    UPPER(col2) as col2,
    COALESCE(col3, 0) as col3
FROM source_table
WHERE condition = true""", height=200)
        if st.button("▶️ Execute Custom SQL", type="primary", use_container_width=True):
            if custom_sql and target_full_name:
                with st.spinner("Executing custom transformation..."):
                    try:
                        session.sql(f"CREATE OR REPLACE TABLE {target_full_name} AS {custom_sql}").collect()
                        st.success(f"✅ Custom transformation completed! Created table: {target_full_name}")
                        st.balloons()
                    except Exception as e:
                        st.error(f"Error executing SQL: {str(e)}")
            else:
                st.warning("Please provide SQL query and target table name")

with tab2:
    st.markdown("### 📋 Recent Transformations")
    try:
        st.markdown("#### Recently Created Tables")
        if source_db and source_schema:
            tables = session.sql(f"""
                SELECT 
                    table_name,
                    row_count,
                    bytes,
                    created
                FROM {source_db}.information_schema.tables
                WHERE table_schema = '{source_schema}'
                ORDER BY created DESC
                LIMIT 20
            """).to_pandas()
            if not tables.empty:
                st.dataframe(
                    tables,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "TABLE_NAME": "Table Name",
                        "ROW_COUNT": st.column_config.NumberColumn("Rows", format="%d"),
                        "BYTES": st.column_config.NumberColumn("Size (bytes)", format="%d"),
                        "CREATED": st.column_config.DatetimeColumn("Created", format="MMM DD, YYYY HH:mm")
                    }
                )
            else:
                st.info("No tables found in the selected schema")
        else:
            st.info("Select a database and schema to view tables")
    except Exception as e:
        st.error(f"Error loading table history: {str(e)}")

with tab3:
    st.markdown("### 💡 Transformation Examples")
    st.markdown("""
    #### Common Use Cases
    **1. Deduplication**
    - Remove duplicate customer records based on email
    - Eliminate duplicate transactions by transaction_id
    - Clean up duplicate product entries

    **2. Null Value Handling**
    - Replace missing prices with 0
    - Remove rows with missing required fields
    - Fill missing categories with 'Unknown'

    **3. Text Standardization**
    - Normalize email addresses to lowercase
    - Remove special characters from phone numbers
    - Trim whitespace from names and addresses

    **4. Data Type Conversions**
    - Convert string dates to proper DATE type
    - Parse JSON fields into columns
    - Cast numeric strings to numbers
    """)
    st.markdown("---")
    st.markdown("#### Sample SQL Transformations")
    with st.expander("📧 Email Standardization"):
        st.code("""
SELECT 
    customer_id,
    LOWER(TRIM(email)) as email,
    first_name,
    last_name
FROM customers
WHERE email IS NOT NULL
        """, language="sql")
    with st.expander("🔢 Numeric Cleaning"):
        st.code("""
SELECT 
    order_id,
    COALESCE(quantity, 0) as quantity,
    COALESCE(TRY_CAST(price AS DECIMAL(10,2)), 0) as price,
    quantity * price as total
FROM orders
        """, language="sql")
    with st.expander("📅 Date Parsing"):
        st.code("""
SELECT 
    transaction_id,
    TRY_TO_DATE(date_string, 'YYYY-MM-DD') as transaction_date,
    amount,
    status
FROM transactions
WHERE TRY_TO_DATE(date_string, 'YYYY-MM-DD') IS NOT NULL
        """, language="sql")
    with st.expander("🏷️ Category Standardization"):
        st.code("""
SELECT 
    product_id,
    product_name,
    CASE 
        WHEN UPPER(category) IN ('ELECTRONICS', 'ELECTRONIC', 'TECH') THEN 'Electronics'
        WHEN UPPER(category) IN ('CLOTHING', 'CLOTHES', 'APPAREL') THEN 'Clothing'
        WHEN UPPER(category) IN ('FOOD', 'GROCERY', 'GROCERIES') THEN 'Food'
        ELSE 'Other'
    END as standardized_category
FROM products
        """, language="sql")
