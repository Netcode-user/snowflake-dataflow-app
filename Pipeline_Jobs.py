import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Pipeline Jobs", page_icon="⚙️", layout="wide")

session = get_active_session()

st.title("⚙️ Data Pipeline Jobs")
st.markdown("Create and manage automated data transformation pipelines")
st.markdown("---")

tab1, tab2, tab3 = st.tabs(["➕ Create Job", "📋 Manage Jobs", "📊 Execution History"])

with tab1:
    st.markdown("### Create New Pipeline Job")
    col1, col2 = st.columns(2)
    with col1:
        job_name = st.text_input("Job Name", placeholder="e.g., Daily Customer Deduplication")
        try:
            databases = session.sql("SHOW DATABASES").collect()
            db_list = [row['name'] for row in databases]
            job_db = st.selectbox("Database", db_list, key="job_db")
            if job_db:
                schemas = session.sql(f"SHOW SCHEMAS IN DATABASE {job_db}").collect()
                schema_list = [row['name'] for row in schemas]
                job_schema = st.selectbox("Schema", schema_list, key="job_schema")
                if job_schema:
                    tables = session.sql(f"SHOW TABLES IN SCHEMA {job_db}.{job_schema}").collect()
                    table_list = [row['name'] for row in tables]
                    source_table = st.selectbox("Source Table", table_list, key="job_source")
                    source_full = f"{job_db}.{job_schema}.{source_table}"
        except Exception as e:
            st.error(f"Error: {str(e)}")
            source_full = ""
    with col2:
        target_table_name = st.text_input("Target Table Name", placeholder="e.g., cleaned_customers")
        if job_db and job_schema and target_table_name:
            target_full = f"{job_db}.{job_schema}.{target_table_name}"
            st.info(f"Target: `{target_full}`")
        else:
            target_full = ""
        transformation_type = st.selectbox("Transformation Type", ["DEDUPLICATE", "CLEAN_NULLS", "STANDARDIZE"])
        is_active = st.checkbox("Active Job", value=True)
    st.markdown("---")
    st.markdown("### Transformation Configuration")
    transformation_config = {}
    if transformation_type == "DEDUPLICATE" and source_full:
        try:
            columns = session.sql(f"SHOW COLUMNS IN TABLE {source_full}").collect()
            column_list = [row['column_name'] for row in columns]
            key_columns = st.multiselect("Key Columns for Deduplication", column_list)
            transformation_config = {"key_columns": key_columns}
        except:
            st.warning("Unable to load columns from source table")
    elif transformation_type == "CLEAN_NULLS" and source_full:
        strategy = st.selectbox("Null Handling Strategy", ["DROP", "FILL_ZERO", "FILL_MEAN"])
        try:
            columns = session.sql(f"SHOW COLUMNS IN TABLE {source_full}").collect()
            column_list = [row['column_name'] for row in columns]
            apply_to_all = st.checkbox("Apply to all columns", value=True)
            if not apply_to_all:
                columns_to_clean = st.multiselect("Columns to Clean", column_list)
            else:
                columns_to_clean = None
            transformation_config = {"strategy": strategy, "columns": columns_to_clean}
        except:
            st.warning("Unable to load columns from source table")
    elif transformation_type == "STANDARDIZE" and source_full:
        try:
            columns = session.sql(f"SHOW COLUMNS IN TABLE {source_full}").collect()
            text_columns = [row['column_name'] for row in columns if 'VARCHAR' in row['type'] or 'TEXT' in row['type'] or 'STRING' in row['type']]
            if text_columns:
                column_name = st.selectbox("Column to Standardize", text_columns)
                operation = st.selectbox("Operation", ["UPPERCASE", "LOWERCASE", "TRIM", "REMOVE_SPECIAL_CHARS"])
                transformation_config = {"column_name": column_name, "operation": operation}
            else:
                st.warning("No text columns found")
        except:
            st.warning("Unable to load columns from source table")
    st.markdown("---")
    st.markdown("### Schedule (Optional)")
    schedule_enabled = st.checkbox("Enable Scheduled Execution")
    if schedule_enabled:
        schedule_type = st.selectbox("Schedule Type", ["Hourly", "Daily", "Weekly", "Monthly", "Custom CRON"])
        if schedule_type == "Custom CRON":
            schedule = st.text_input("CRON Expression", placeholder="0 0 * * *")
        else:
            if schedule_type == "Hourly":
                schedule = "0 * * * *"
            elif schedule_type == "Daily":
                hour = st.slider("Hour (24h)", 0, 23, 0)
                schedule = f"0 {hour} * * *"
            elif schedule_type == "Weekly":
                day = st.selectbox("Day", ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
                day_map = {"Monday": 1, "Tuesday": 2, "Wednesday": 3, "Thursday": 4, "Friday": 5, "Saturday": 6, "Sunday": 0}
                hour = st.slider("Hour (24h)", 0, 23, 0)
                schedule = f"0 {hour} * * {day_map[day]}"
            elif schedule_type == "Monthly":
                day_of_month = st.slider("Day of Month", 1, 28, 1)
                hour = st.slider("Hour (24h)", 0, 23, 0)
                schedule = f"0 {hour} {day_of_month} * *"
        st.info(f"CRON Schedule: `{schedule}`")
    else:
        schedule = None
    st.markdown("---")
    if st.button("💾 Create Pipeline Job", type="primary", use_container_width=True):
        if job_name and source_full and target_full and transformation_config:
            try:
                config_json = json.dumps(transformation_config)
                session.sql(f"""
                    INSERT INTO app_schema.transformation_jobs 
                    (job_name, source_table, target_table, transformation_type, transformation_config, schedule, is_active)
                    VALUES (
                        '{job_name}',
                        '{source_full}',
                        '{target_full}',
                        '{transformation_type}',
                        PARSE_JSON('{config_json}'),
                        {'NULL' if not schedule else f"'{schedule}'"},
                        {is_active}
                    )
                """).collect()
                st.success(f"✅ Pipeline job '{job_name}' created successfully!")
                st.balloons()
            except Exception as e:
                st.error(f"Error creating job: {str(e)}")
        else:
            st.warning("Please fill in all required fields")

with tab2:
    st.markdown("### Manage Pipeline Jobs")
    try:
        jobs_df = session.sql("""
            SELECT 
                job_id,
                job_name,
                source_table,
                target_table,
                transformation_type,
                schedule,
                is_active,
                last_run,
                created_at
            FROM app_schema.transformation_jobs
            ORDER BY created_at DESC
        """).to_pandas()
        if not jobs_df.empty:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Jobs", len(jobs_df))
            with col2:
                active_count = len(jobs_df[jobs_df['IS_ACTIVE'] == True])
                st.metric("Active Jobs", active_count)
            with col3:
                scheduled_count = len(jobs_df[jobs_df['SCHEDULE'].notna()])
                st.metric("Scheduled Jobs", scheduled_count)
            st.markdown("---")
            for idx, job in jobs_df.iterrows():
                with st.expander(f"{'🟢' if job['IS_ACTIVE'] else '🔴'} {job['JOB_NAME']}", expanded=False):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**Job ID:** `{job['JOB_ID']}`")
                        st.markdown(f"**Type:** {job['TRANSFORMATION_TYPE']}")
                        st.markdown(f"**Source:** `{job['SOURCE_TABLE']}`")
                        st.markdown(f"**Target:** `{job['TARGET_TABLE']}`")
                    with col2:
                        st.markdown(f"**Status:** {'✅ Active' if job['IS_ACTIVE'] else '⏸️ Inactive'}")
                        st.markdown(f"**Schedule:** {job['SCHEDULE'] if job['SCHEDULE'] else 'Manual'}")
                        st.markdown(f"**Last Run:** {job['LAST_RUN'] if job['LAST_RUN'] else 'Never'}")
                        st.markdown(f"**Created:** {job['CREATED_AT']}")
                    st.markdown("---")
                    action_col1, action_col2, action_col3 = st.columns(3)
                    with action_col1:
                        if st.button(f"▶️ Run Now", key=f"run_{job['JOB_ID']}"):
                            with st.spinner("Executing job..."):
                                try:
                                    result = session.call("app_schema.execute_transformation_job", job['JOB_ID'])
                                    st.success(result)
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                    with action_col2:
                        new_status = not job['IS_ACTIVE']
                        action_label = "⏸️ Deactivate" if job['IS_ACTIVE'] else "▶️ Activate"
                        if st.button(action_label, key=f"toggle_{job['JOB_ID']}"):
                            try:
                                session.sql(f"UPDATE app_schema.transformation_jobs SET is_active = {new_status} WHERE job_id = '{job['JOB_ID']}'").collect()
                                st.success("Job status updated!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {str(e)}")
                    with action_col3:
                        if st.button(f"🗑️ Delete", key=f"delete_{job['JOB_ID']}"):
                            try:
                                session.sql(f"DELETE FROM app_schema.transformation_jobs WHERE job_id = '{job['JOB_ID']}'").collect()
                                st.success("Job deleted!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {str(e)}")
        else:
            st.info("No pipeline jobs configured yet. Create your first job in the 'Create Job' tab!")
    except Exception as e:
        st.error(f"Error loading jobs: {str(e)}")

with tab3:
    st.markdown("### Job Execution History")
    try:
        history_df = session.sql("""
            SELECT 
                h.execution_id,
                j.job_name,
                h.started_at,
                h.completed_at,
                h.status,
                h.rows_processed,
                h.rows_affected,
                h.execution_time_seconds,
                h.error_message
            FROM app_schema.job_execution_history h
            JOIN app_schema.transformation_jobs j ON h.job_id = j.job_id
            ORDER BY h.started_at DESC
            LIMIT 100
        """).to_pandas()
        if not history_df.empty:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                total_executions = len(history_df)
                st.metric("Total Executions", total_executions)
            with col2:
                success_count = len(history_df[history_df['STATUS'] == 'SUCCESS'])
                success_rate = (success_count / total_executions * 100) if total_executions > 0 else 0
                st.metric("Success Rate", f"{success_rate:.1f}%")
            with col3:
                avg_time = history_df['EXECUTION_TIME_SECONDS'].mean()
                st.metric("Avg Duration", f"{avg_time:.1f}s")
            with col4:
                total_rows = history_df['ROWS_PROCESSED'].sum()
                st.metric("Total Rows Processed", f"{total_rows:,.0f}")
            st.markdown("---")
            col1, col2 = st.columns(2)
            with col1:
                status_counts = history_df['STATUS'].value_counts()
                fig = px.pie(
                    values=status_counts.values,
                    names=status_counts.index,
                    title="Execution Status Distribution",
                    color=status_counts.index,
                    color_discrete_map={'SUCCESS': '#28a745', 'FAILED': '#dc3545', 'RUNNING': '#ffc107'}
                )
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                job_counts = history_df.groupby('JOB_NAME').size().reset_index(name='count')
                fig = px.bar(job_counts, x='JOB_NAME', y='count', title="Executions by Job", labels={'JOB_NAME': 'Job', 'count': 'Executions'})
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            st.markdown("---")
            st.markdown("#### Detailed Execution Log")
            history_df['STATUS_DISPLAY'] = history_df['STATUS'].map({'SUCCESS': '✅ Success','FAILED': '❌ Failed','RUNNING': '⏳ Running'})
            st.dataframe(
                history_df[['JOB_NAME', 'STATUS_DISPLAY', 'STARTED_AT', 'EXECUTION_TIME_SECONDS','ROWS_PROCESSED', 'ROWS_AFFECTED', 'ERROR_MESSAGE']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "JOB_NAME": "Job",
                    "STATUS_DISPLAY": "Status",
                    "STARTED_AT": st.column_config.DatetimeColumn("Started", format="MMM DD HH:mm:ss"),
                    "EXECUTION_TIME_SECONDS": st.column_config.NumberColumn("Duration (s)", format="%.2f"),
                    "ROWS_PROCESSED": st.column_config.NumberColumn("Processed", format="%d"),
                    "ROWS_AFFECTED": st.column_config.NumberColumn("Affected", format="%d"),
                    "ERROR_MESSAGE": "Error"
                }
            )
            if st.button("📥 Export History"):
                csv = history_df.to_csv(index=False)
                st.download_button(label="Download CSV", data=csv, file_name="job_execution_history.csv", mime="text/csv")
        else:
            st.info("No execution history yet. Run some jobs to see their execution history!")
    except Exception as e:
        st.error(f"Error loading execution history: {str(e)}")
