import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import plotly.express as px
import json

st.set_page_config(page_title="Quality Checks", page_icon="✅", layout="wide")

session = get_active_session()

st.title("✅ Data Quality Checks")
st.markdown("Configure and run automated quality checks to ensure data reliability")
st.markdown("---")

tab1, tab2, tab3 = st.tabs(["➕ Create Check", "▶️ Run Checks", "📊 Results"])

with tab1:
    st.markdown("### Create New Quality Check")
    col1, col2 = st.columns(2)
    with col1:
        check_name = st.text_input("Check Name", placeholder="e.g., Customer Email Validation")
        try:
            databases = session.sql("SHOW DATABASES").collect()
            db_list = [row['name'] for row in databases]
            selected_db = st.selectbox("Database", db_list, key="check_db")
            if selected_db:
                schemas = session.sql(f"SHOW SCHEMAS IN DATABASE {selected_db}").collect()
                schema_list = [row['name'] for row in schemas]
                selected_schema = st.selectbox("Schema", schema_list, key="check_schema")
                if selected_schema:
                    tables = session.sql(f"SHOW TABLES IN SCHEMA {selected_db}.{selected_schema}").collect()
                    table_list = [row['name'] for row in tables]
                    table_name = st.selectbox("Table", table_list, key="check_table")
                    full_table_name = f"{selected_db}.{selected_schema}.{table_name}"
                    if table_name:
                        columns = session.sql(f"SHOW COLUMNS IN TABLE {full_table_name}").collect()
                        column_list = [row['column_name'] for row in columns]
                        column_name = st.selectbox("Column (optional for table-level checks)", [""] + column_list)
        except Exception as e:
            st.error(f"Error loading database objects: {str(e)}")
            full_table_name = ""
            column_name = ""

    with col2:
        check_type = st.selectbox("Check Type", ["NULL_CHECK", "DUPLICATE_CHECK", "RANGE_CHECK", "PATTERN_CHECK", "UNIQUENESS_CHECK"])
        severity = st.selectbox("Severity", ["INFO", "WARNING", "ERROR", "CRITICAL"])
        check_params = {}
        if check_type == "RANGE_CHECK":
            st.markdown("##### Range Parameters")
            min_val = st.number_input("Minimum Value", value=0.0)
            max_val = st.number_input("Maximum Value", value=100.0)
            check_params = {"min_value": min_val, "max_value": max_val}
        elif check_type == "PATTERN_CHECK":
            st.markdown("##### Pattern Parameters")
            pattern = st.text_input("Regex Pattern", placeholder="e.g., ^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$")
            check_params = {"pattern": pattern}
        is_active = st.checkbox("Active", value=True)

    if st.button("💾 Create Quality Check", type="primary", use_container_width=True):
        if check_name and full_table_name and check_type:
            try:
                params_json = json.dumps(check_params) if check_params else "{}"
                session.sql(f"""
                    INSERT INTO app_schema.quality_check_configs 
                    (check_name, table_name, column_name, check_type, check_parameters, severity, is_active)
                    VALUES (
                        '{check_name}',
                        '{full_table_name}',
                        '{column_name}',
                        '{check_type}',
                        PARSE_JSON('{params_json}'),
                        '{severity}',
                        {is_active}
                    )
                """).collect()
                st.success(f"✅ Quality check '{check_name}' created successfully!")
                st.balloons()
            except Exception as e:
                st.error(f"Error creating quality check: {str(e)}")
        else:
            st.warning("Please fill in all required fields")

with tab2:
    st.markdown("### Run Quality Checks")
    try:
        checks_df = session.sql("""
            SELECT 
                check_id,
                check_name,
                table_name,
                column_name,
                check_type,
                severity,
                is_active
            FROM app_schema.quality_check_configs
            ORDER BY created_at DESC
        """).to_pandas()
        if not checks_df.empty:
            st.dataframe(
                checks_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "CHECK_ID": "ID",
                    "CHECK_NAME": "Check Name",
                    "TABLE_NAME": "Table",
                    "COLUMN_NAME": "Column",
                    "CHECK_TYPE": "Type",
                    "SEVERITY": "Severity",
                    "IS_ACTIVE": st.column_config.CheckboxColumn("Active")
                }
            )
            st.markdown("---")
            col1, col2 = st.columns([2, 1])
            with col1:
                unique_tables = checks_df['TABLE_NAME'].unique().tolist()
                selected_check_table = st.selectbox("Select Table to Check", unique_tables)
            with col2:
                if st.button("▶️ Run All Checks for Table", type="primary", use_container_width=True):
                    with st.spinner(f"Running quality checks on {selected_check_table}..."):
                        try:
                            result = session.call("app_schema.run_quality_checks", selected_check_table)
                            st.success(result)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error running checks: {str(e)}")
            st.markdown("---")
            st.markdown("#### Quick Actions")
            action_col1, action_col2 = st.columns(2)
            with action_col1:
                if st.button("▶️ Run All Active Checks", use_container_width=True):
                    with st.spinner("Running all active quality checks..."):
                        try:
                            active_tables = checks_df[checks_df['IS_ACTIVE']]['TABLE_NAME'].unique()
                            for table in active_tables:
                                session.call("app_schema.run_quality_checks", table)
                            st.success(f"✅ Completed checks on {len(active_tables)} tables")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {str(e)}")
            with action_col2:
                if st.button("🔄 Refresh Results", use_container_width=True):
                    st.rerun()
        else:
            st.info("No quality checks configured yet. Create your first check in the 'Create Check' tab!")
    except Exception as e:
        st.error(f"Error loading quality checks: {str(e)}")

with tab3:
    st.markdown("### Quality Check Results")
    try:
        results_df = session.sql("""
            SELECT 
                r.result_id,
                c.check_name,
                c.table_name,
                c.column_name,
                c.check_type,
                r.status,
                r.records_checked,
                r.records_failed,
                r.failure_rate,
                r.execution_time,
                c.severity
            FROM app_schema.quality_check_results r
            JOIN app_schema.quality_check_configs c ON r.check_id = c.check_id
            ORDER BY r.execution_time DESC
            LIMIT 100
        """).to_pandas()
        if not results_df.empty:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                total_checks = len(results_df)
                st.metric("Total Checks Run", total_checks)
            with col2:
                passed = len(results_df[results_df['STATUS'] == 'PASSED'])
                st.metric("Passed", passed, delta=f"{(passed/total_checks*100):.1f}%")
            with col3:
                failed = len(results_df[results_df['STATUS'] == 'FAILED'])
                st.metric("Failed", failed, delta=f"{(failed/total_checks*100):.1f}%")
            with col4:
                avg_failure_rate = results_df['FAILURE_RATE'].mean()
                st.metric("Avg Failure Rate", f"{avg_failure_rate:.2f}%")
            st.markdown("---")
            col1, col2, col3 = st.columns(3)
            with col1:
                status_filter = st.multiselect(
                    "Filter by Status",
                    options=results_df['STATUS'].unique().tolist(),
                    default=results_df['STATUS'].unique().tolist()
                )
            with col2:
                table_filter = st.multiselect(
                    "Filter by Table",
                    options=results_df['TABLE_NAME'].unique().tolist(),
                    default=results_df['TABLE_NAME'].unique().tolist()
                )
            with col3:
                severity_filter = st.multiselect(
                    "Filter by Severity",
                    options=results_df['SEVERITY'].unique().tolist(),
                    default=results_df['SEVERITY'].unique().tolist()
                )
            filtered_df = results_df[
                (results_df['STATUS'].isin(status_filter)) &
                (results_df['TABLE_NAME'].isin(table_filter)) &
                (results_df['SEVERITY'].isin(severity_filter))
            ]
            st.markdown("#### Results Over Time")
            col1, col2 = st.columns(2)
            with col1:
                status_counts = filtered_df['STATUS'].value_counts()
                fig = px.pie(
                    values=status_counts.values,
                    names=status_counts.index,
                    title="Check Status Distribution",
                    color=status_counts.index,
                    color_discrete_map={'PASSED': '#28a745', 'FAILED': '#dc3545', 'WARNING': '#ffc107'}
                )
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                severity_counts = filtered_df['SEVERITY'].value_counts()
                fig = px.pie(
                    values=severity_counts.values,
                    names=severity_counts.index,
                    title="Severity Distribution",
                    color_discrete_sequence=px.colors.sequential.Reds
                )
                st.plotly_chart(fig, use_container_width=True)
            st.markdown("#### Detailed Results")
            filtered_df['STATUS_DISPLAY'] = filtered_df['STATUS'].map({
                'PASSED': '✅ Passed',
                'FAILED': '❌ Failed',
                'WARNING': '⚠️ Warning'
            })
            st.dataframe(
                filtered_df[[
                    'CHECK_NAME', 'TABLE_NAME', 'COLUMN_NAME', 'CHECK_TYPE',
                    'STATUS_DISPLAY', 'RECORDS_CHECKED', 'RECORDS_FAILED',
                    'FAILURE_RATE', 'SEVERITY', 'EXECUTION_TIME'
                ]],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "CHECK_NAME": "Check",
                    "TABLE_NAME": "Table",
                    "COLUMN_NAME": "Column",
                    "CHECK_TYPE": "Type",
                    "STATUS_DISPLAY": "Status",
                    "RECORDS_CHECKED": st.column_config.NumberColumn("Checked", format="%d"),
                    "RECORDS_FAILED": st.column_config.NumberColumn("Failed", format="%d"),
                    "FAILURE_RATE": st.column_config.NumberColumn("Failure Rate", format="%.2f%%"),
                    "SEVERITY": "Severity",
                    "EXECUTION_TIME": st.column_config.DatetimeColumn("Time", format="MMM DD HH:mm")
                }
            )
            if st.button("📥 Export Results"):
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name="quality_check_results.csv",
                    mime="text/csv"
                )
        else:
            st.info("No quality check results yet. Run some checks in the 'Run Checks' tab!")
    except Exception as e:
        st.error(f"Error loading results: {str(e)}")
