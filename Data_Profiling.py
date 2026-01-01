import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Data Profiling", page_icon="🔍", layout="wide")

session = get_active_session()

st.title("🔍 Data Profiling")
st.markdown("Automatically analyze your tables to understand data quality, distributions, and characteristics")
st.markdown("---")

col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("### Profile a Table")
    try:
        databases = session.sql("SHOW DATABASES").collect()
        db_list = [row['name'] for row in databases]
        
        selected_db = st.selectbox("Select Database", db_list, key="profile_db")
        
        if selected_db:
            schemas = session.sql(f"SHOW SCHEMAS IN DATABASE {selected_db}").collect()
            schema_list = [row['name'] for row in schemas]
            selected_schema = st.selectbox("Select Schema", schema_list, key="profile_schema")
            
            if selected_schema:
                tables = session.sql(f"SHOW TABLES IN SCHEMA {selected_db}.{selected_schema}").collect()
                table_list = [row['name'] for row in tables]
                selected_table = st.selectbox("Select Table", table_list, key="profile_table")
                
                sample_size = st.slider("Sample Size for Examples", 10, 500, 100, 10)
                
                if st.button("🔍 Profile Table", type="primary", use_container_width=True):
                    with st.spinner("Profiling table... This may take a moment"):
                        try:
                            full_table_name = f"{selected_db}.{selected_schema}.{selected_table}"
                            result = session.call("app_schema.profile_table", full_table_name, sample_size)
                            st.success(result)
                            st.balloons()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error profiling table: {str(e)}")
    except Exception as e:
        st.error(f"Error loading database objects: {str(e)}")

with col2:
    st.markdown("### 📊 Quick Stats")
    try:
        total_profiles = session.sql("SELECT COUNT(DISTINCT table_name) as count FROM app_schema.data_profile_results").collect()[0]['COUNT']
        total_columns = session.sql("SELECT COUNT(*) as count FROM app_schema.data_profile_results").collect()[0]['COUNT']
        avg_null_pct = session.sql("SELECT ROUND(AVG(null_percentage), 2) as avg_pct FROM app_schema.data_profile_results").collect()[0]['AVG_PCT'] or 0
        
        st.metric("Tables Profiled", total_profiles)
        st.metric("Total Columns Analyzed", total_columns)
        st.metric("Avg Null %", f"{avg_null_pct}%")
    except:
        st.info("No profiling data available yet")

st.markdown("---")
st.markdown("### 📋 Profiled Tables")

try:
    profiled_tables = session.sql("SELECT DISTINCT table_name FROM app_schema.data_profile_results ORDER BY table_name").to_pandas()
    
    if not profiled_tables.empty:
        selected_profiled_table = st.selectbox(
            "Select a profiled table to view details:",
            profiled_tables['TABLE_NAME'].tolist()
        )
        
        if selected_profiled_table:
            profile_df = session.sql(f"""
                SELECT 
                    column_name,
                    data_type,
                    row_count,
                    null_count,
                    null_percentage,
                    distinct_count,
                    distinct_percentage,
                    min_value,
                    max_value,
                    avg_value,
                    profiled_at
                FROM app_schema.data_profile_results
                WHERE table_name = '{selected_profiled_table}'
                ORDER BY column_name
            """).to_pandas()
            
            st.markdown(f"#### Summary for `{selected_profiled_table}`")
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Rows", f"{profile_df['ROW_COUNT'].iloc[0]:,}")
            with col2:
                st.metric("Total Columns", len(profile_df))
            with col3:
                high_null_cols = len(profile_df[profile_df['NULL_PERCENTAGE'] > 50])
                st.metric("High Null Columns (>50%)", high_null_cols)
            with col4:
                low_cardinality = len(profile_df[profile_df['DISTINCT_PERCENTAGE'] < 1])
                st.metric("Low Cardinality Columns (<1%)", low_cardinality)
            
            st.markdown("---")
            
            tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📈 Null Analysis", "🎯 Cardinality", "📋 Details"])
            
            with tab1:
                display_df = profile_df[['COLUMN_NAME', 'DATA_TYPE', 'NULL_PERCENTAGE', 'DISTINCT_COUNT', 'DISTINCT_PERCENTAGE']].copy()
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "COLUMN_NAME": "Column",
                        "DATA_TYPE": "Type",
                        "NULL_PERCENTAGE": st.column_config.NumberColumn("Null %", format="%.2f%%"),
                        "DISTINCT_COUNT": "Distinct Values",
                        "DISTINCT_PERCENTAGE": st.column_config.NumberColumn("Distinct %", format="%.2f%%")
                    }
                )
            
            with tab2:
                null_chart_df = profile_df[['COLUMN_NAME', 'NULL_PERCENTAGE']].sort_values('NULL_PERCENTAGE', ascending=False)
                fig = px.bar(
                    null_chart_df,
                    x='COLUMN_NAME',
                    y='NULL_PERCENTAGE',
                    title="Null Percentage by Column",
                    labels={'COLUMN_NAME': 'Column', 'NULL_PERCENTAGE': 'Null %'},
                    color='NULL_PERCENTAGE',
                    color_continuous_scale=['green', 'yellow', 'red']
                )
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
                
                high_null_df = profile_df[profile_df['NULL_PERCENTAGE'] > 20][['COLUMN_NAME', 'NULL_PERCENTAGE', 'NULL_COUNT']]
                if not high_null_df.empty:
                    st.warning(f"⚠️ {len(high_null_df)} columns have >20% null values")
                    st.dataframe(high_null_df, use_container_width=True, hide_index=True)
            
            with tab3:
                card_chart_df = profile_df[['COLUMN_NAME', 'DISTINCT_PERCENTAGE']].sort_values('DISTINCT_PERCENTAGE', ascending=False)
                fig = px.bar(
                    card_chart_df,
                    x='COLUMN_NAME',
                    y='DISTINCT_PERCENTAGE',
                    title="Distinct Value Percentage by Column",
                    labels={'COLUMN_NAME': 'Column', 'DISTINCT_PERCENTAGE': 'Distinct %'},
                    color='DISTINCT_PERCENTAGE',
                    color_continuous_scale='blues'
                )
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
                
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("##### Potential Key Columns (>95% unique)")
                    key_candidates = profile_df[profile_df['DISTINCT_PERCENTAGE'] > 95][['COLUMN_NAME', 'DISTINCT_PERCENTAGE']]
                    if not key_candidates.empty:
                        st.dataframe(key_candidates, use_container_width=True, hide_index=True)
                    else:
                        st.info("No columns with >95% distinct values")
                
                with col2:
                    st.markdown("##### Low Cardinality Columns (<5% unique)")
                    low_card = profile_df[profile_df['DISTINCT_PERCENTAGE'] < 5][['COLUMN_NAME', 'DISTINCT_COUNT']]
                    if not low_card.empty:
                        st.dataframe(low_card, use_container_width=True, hide_index=True)
                    else:
                        st.info("No columns with <5% distinct values")
            
            with tab4:
                st.markdown("#### Detailed Statistics")
                st.dataframe(
                    profile_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "COLUMN_NAME": "Column",
                        "DATA_TYPE": "Type",
                        "ROW_COUNT": st.column_config.NumberColumn("Rows", format="%d"),
                        "NULL_COUNT": st.column_config.NumberColumn("Nulls", format="%d"),
                        "NULL_PERCENTAGE": st.column_config.NumberColumn("Null %", format="%.2f%%"),
                        "DISTINCT_COUNT": st.column_config.NumberColumn("Distinct", format="%d"),
                        "DISTINCT_PERCENTAGE": st.column_config.NumberColumn("Distinct %", format="%.2f%%"),
                        "MIN_VALUE": "Min",
                        "MAX_VALUE": "Max",
                        "AVG_VALUE": st.column_config.NumberColumn("Average", format="%.2f"),
                        "PROFILED_AT": st.column_config.DatetimeColumn("Profiled", format="MMM DD, YYYY HH:mm")
                    }
                )
                
                if st.button("📥 Export Profile Data"):
                    csv = profile_df.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"{selected_profiled_table}_profile.csv",
                        mime="text/csv"
                    )
    else:
        st.info("No tables have been profiled yet. Use the form above to profile your first table!")
        
except Exception as e:
    st.error(f"Error displaying profiled tables: {str(e)}")
