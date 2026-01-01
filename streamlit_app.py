import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

st.set_page_config(
    page_title="DataFlow Pro",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

session = get_active_session()

st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .success-box {
        background-color: #d4edda;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #28a745;
    }
    .warning-box {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #ffc107;
    }
    .error-box {
        background-color: #f8d7da;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #dc3545;
    }
    </style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.image("https://via.placeholder.com/150x50/1f77b4/ffffff?text=DataFlow+Pro", use_container_width=True)
    st.markdown("---")
    st.markdown("### 🎯 Quick Actions")
    if st.button("🔍 Profile New Table", use_container_width=True):
        st.switch_page("pages/1_Data_Profiling.py")
    if st.button("✅ Run Quality Checks", use_container_width=True):
        st.switch_page("pages/2_Quality_Checks.py")
    if st.button("🔄 Create Transformation", use_container_width=True):
        st.switch_page("pages/3_Transformations.py")
    if st.button("⚙️ Manage Jobs", use_container_width=True):
        st.switch_page("pages/4_Pipeline_Jobs.py")
    st.markdown("---")
    st.markdown("### 📚 Resources")
    st.markdown("[Documentation](#) | [Support](#) | [API](#)")

st.markdown('<p class="main-header">📊 DataFlow Pro Dashboard</p>', unsafe_allow_html=True)
st.markdown("**Your centralized hub for data operations, quality monitoring, and automation**")
st.markdown("---")

try:
    tables_profiled = session.sql("""
        SELECT COUNT(DISTINCT table_name) as count 
        FROM app_schema.data_profile_results
    """).collect()[0]['COUNT']
    
    recent_checks = session.sql("""
        SELECT COUNT(*) as count 
        FROM app_schema.quality_check_results 
        WHERE execution_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
    """).collect()[0]['COUNT']
    
    active_jobs = session.sql("""
        SELECT COUNT(*) as count 
        FROM app_schema.transformation_jobs 
        WHERE is_active = TRUE
    """).collect()[0]['COUNT']
    
    job_success_rate = session.sql("""
        SELECT 
            ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) as rate
        FROM app_schema.job_execution_history
        WHERE started_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    """).collect()[0]['RATE'] or 0
    
except Exception as e:
    st.error(f"Error fetching metrics: {str(e)}")
    tables_profiled = 0
    recent_checks = 0
    active_jobs = 0
    job_success_rate = 0

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(label="📋 Tables Profiled", value=tables_profiled, delta="Total")
with col2:
    st.metric(label="✅ Quality Checks (24h)", value=recent_checks, delta="Recent")
with col3:
    st.metric(label="⚙️ Active Jobs", value=active_jobs, delta="Running")
with col4:
    st.metric(label="📈 Success Rate (7d)", value=f"{job_success_rate}%", delta="Week")

st.markdown("---")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 📊 Recent Quality Check Results")
    try:
        quality_data = session.sql("""
            SELECT 
                DATE(execution_time) as check_date,
                status,
                COUNT(*) as count
            FROM app_schema.quality_check_results
            WHERE execution_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
            GROUP BY check_date, status
            ORDER BY check_date
        """).to_pandas()
        
        if not quality_data.empty:
            fig = px.bar(
                quality_data,
                x='CHECK_DATE',
                y='COUNT',
                color='STATUS',
                title="Quality Check Status (Last 7 Days)",
                color_discrete_map={'PASSED': '#28a745', 'FAILED': '#dc3545', 'WARNING': '#ffc107'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No quality check data available for the last 7 days")
    except Exception:
        st.warning("No quality check history available yet")

with col2:
    st.markdown("### ⚙️ Job Execution Trends")
    try:
        job_data = session.sql("""
            SELECT 
                DATE(started_at) as execution_date,
                status,
                COUNT(*) as count
            FROM app_schema.job_execution_history
            WHERE started_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
            GROUP BY execution_date, status
            ORDER BY execution_date
        """).to_pandas()
        
        if not job_data.empty:
            fig = px.line(
                job_data,
                x='EXECUTION_DATE',
                y='COUNT',
                color='STATUS',
                title="Job Execution Status (Last 7 Days)",
                color_discrete_map={'SUCCESS': '#28a745', 'FAILED': '#dc3545', 'RUNNING': '#ffc107'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No job execution data available for the last 7 days")
    except Exception:
        st.warning("No job execution history available yet")

st.markdown("---")
st.markdown("### 📋 Recent Activity")

tab1, tab2 = st.tabs(["Latest Profiles", "Recent Jobs"])

with tab1:
    try:
        latest_profiles = session.sql("""
            SELECT 
                table_name,
                COUNT(DISTINCT column_name) as columns_profiled,
                MAX(profiled_at) as last_profiled
            FROM app_schema.data_profile_results
            GROUP BY table_name
            ORDER BY last_profiled DESC
            LIMIT 10
        """).to_pandas()
        
        if not latest_profiles.empty:
            st.dataframe(
                latest_profiles,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "TABLE_NAME": "Table",
                    "COLUMNS_PROFILED": "Columns",
                    "LAST_PROFILED": st.column_config.DatetimeColumn("Last Profiled", format="MMM DD, YYYY HH:mm")
                }
            )
        else:
            st.info("No profiling activity yet. Start by profiling a table!")
    except Exception:
        st.warning("No profiling history available")

with tab2:
    try:
        recent_jobs = session.sql("""
            SELECT 
                j.job_name,
                h.status,
                h.rows_processed,
                h.execution_time_seconds,
                h.started_at
            FROM app_schema.job_execution_history h
            JOIN app_schema.transformation_jobs j ON h.job_id = j.job_id
            ORDER BY h.started_at DESC
            LIMIT 10
        """).to_pandas()
        
        if not recent_jobs.empty:
            recent_jobs['STATUS_DISPLAY'] = recent_jobs['STATUS'].map({
                'SUCCESS': '✅ Success',
                'FAILED': '❌ Failed',
                'RUNNING': '⏳ Running'
            })
            
            st.dataframe(
                recent_jobs[['JOB_NAME', 'STATUS_DISPLAY', 'ROWS_PROCESSED', 'EXECUTION_TIME_SECONDS', 'STARTED_AT']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "JOB_NAME": "Job Name",
                    "STATUS_DISPLAY": "Status",
                    "ROWS_PROCESSED": "Rows Processed",
                    "EXECUTION_TIME_SECONDS": "Duration (s)",
                    "STARTED_AT": st.column_config.DatetimeColumn("Started", format="MMM DD, YYYY HH:mm")
                }
            )
        else:
            st.info("No job executions yet. Create a transformation job to get started!")
    except Exception:
        st.warning("No job execution history available")

st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #666; padding: 1rem;'>
        <p>DataFlow Pro v1.0 | Built on Snowflake Native Apps | 
        <a href='#'>Documentation</a> | <a href='#'>Support</a></p>
    </div>
""", unsafe_allow_html=True)
