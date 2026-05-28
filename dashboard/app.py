import streamlit as st
import pandas as pd
import numpy as np
import pymongo
import psycopg2
import time

# --- Configuration ---
st.set_page_config(page_title="Walmart Real-Time Dashboard", layout="wide", page_icon="🛒")

# Database Credentials
MONGO_URI = "mongodb://admin:admin@localhost:27017/"
MONGO_DB = "walmart"

POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5433"
POSTGRES_DB = "airflow"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"

@st.cache_resource
def get_mongo_client():
    return pymongo.MongoClient(MONGO_URI)

def fetch_mongo_store_agg():
    """Fetches store aggregation data from MongoDB."""
    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        cursor = db["sales_agg_by_store"].find({}, {'_id': 0})
        df = pd.DataFrame(list(cursor))
        return df
    except Exception:
        return pd.DataFrame()

def fetch_mongo_holiday_agg():
    """Fetches holiday aggregation data from MongoDB."""
    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        cursor = db["sales_agg_by_holiday"].find({}, {'_id': 0})
        df = pd.DataFrame(list(cursor))
        return df
    except Exception:
        return pd.DataFrame()

def fetch_postgres_data():
    """Fetches ML prediction data from PostgreSQL."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        query = 'SELECT * FROM sales_predictions ORDER BY created_at DESC LIMIT 1000'
        df = pd.read_sql_query(query, conn)
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

# --- Season & Festive Helpers (defined once) ---
def get_season(month):
    """Maps a month number to a season name."""
    if pd.isna(month):
        return "Unknown"
    if month in [12, 1, 2]:
        return "Winter"
    elif month in [3, 4, 5]:
        return "Spring"
    elif month in [6, 7, 8]:
        return "Summer"
    else:
        return "Autumn"

def get_holiday_name(row):
    """Maps a row to its festive event name based on date and holiday flag."""
    is_holiday = row['is_holiday']
    if not is_holiday or is_holiday in [0, '0', False, 'False', 'false']:
        return "Regular Days"
    dt = row['Date_Parsed']
    if pd.isna(dt):
        return "Holiday"
    week = dt.isocalendar().week
    month = dt.month
    if month == 2 and week in [5, 6, 7]:
        return "Super Bowl"
    elif month == 9 and week in [35, 36, 37]:
        return "Labor Day"
    elif month == 11 and week in [46, 47, 48]:
        return "Thanksgiving"
    elif month == 12 and week in [51, 52]:
        return "Christmas"
    else:
        return "Other Holiday"

# --- Main App ---
st.title("🛒 Walmart Real-Time Sales & ML Predictions")

with st.spinner("Fetching live data from streaming databases..."):
    store_agg = fetch_mongo_store_agg()
    holiday_agg = fetch_mongo_holiday_agg()
    pg_df = fetch_postgres_data()

# --- Sidebar Filters ---
st.sidebar.header("Dashboard Filters")

has_data = not pg_df.empty or not store_agg.empty

if has_data:
    # --- PostgreSQL Predictions Section ---
    if not pg_df.empty:
        stores = sorted(pg_df['store'].unique().tolist())
        selected_store = st.sidebar.selectbox("Select Store", ["All Stores"] + stores)
        holiday_filter = st.sidebar.radio("Holiday Filter", ["All", "Holiday Only", "Non-Holiday Only"])

        filtered_df = pg_df.copy()
        if selected_store != "All Stores":
            filtered_df = filtered_df[filtered_df['store'] == selected_store]
        if holiday_filter == "Holiday Only":
            filtered_df = filtered_df[filtered_df['is_holiday'] == True]
        elif holiday_filter == "Non-Holiday Only":
            filtered_df = filtered_df[filtered_df['is_holiday'] == False]

        # =====================================================
        # ROW 1: Core KPI Metrics
        # =====================================================
        col_m1, col_m2, col_m3, col_m4 = st.columns(4)
        col_m1.metric("📊 Total Records", f"{len(filtered_df):,}")
        col_m2.metric("🏪 Stores Active", f"{filtered_df['store'].nunique()}")

        avg_actual = filtered_df['weekly_sales'].mean() if len(filtered_df) > 0 else 0
        avg_pred = filtered_df['predicted_sales'].mean() if len(filtered_df) > 0 else 0
        col_m3.metric("💰 Avg Actual Sales", f"${avg_actual:,.0f}")
        col_m4.metric("🤖 Avg Predicted Sales", f"${avg_pred:,.0f}")

        # =====================================================
        # ROW 2: ML Model Performance Metrics
        # =====================================================
        col_q1, col_q2, col_q3, col_q4 = st.columns(4)
        if len(filtered_df) > 0:
            mape_df = filtered_df[filtered_df['weekly_sales'] != 0]
            mape = ((mape_df['weekly_sales'] - mape_df['predicted_sales']).abs() / mape_df['weekly_sales']).mean() * 100 if not mape_df.empty else 0

            mae = (filtered_df['weekly_sales'] - filtered_df['predicted_sales']).abs().mean()
            accuracy = max(0.0, 100.0 - mape)
            total_variance = (filtered_df['weekly_sales'] - filtered_df['predicted_sales']).sum()

            col_q1.metric("🎯 Model Accuracy", f"{accuracy:.2f}%")
            col_q2.metric("📐 Mean Abs Error", f"${mae:,.2f}")
            col_q3.metric("📉 MAPE", f"{mape:.2f}%")
            col_q4.metric("⚖️ Total Variance", f"${total_variance:,.0f}", delta=f"${total_variance:,.0f}", delta_color="inverse")
        else:
            col_q1.metric("🎯 Model Accuracy", "N/A")
            col_q2.metric("📐 Mean Abs Error", "N/A")
            col_q3.metric("📉 MAPE", "N/A")
            col_q4.metric("⚖️ Total Variance", "N/A")

        st.markdown("---")

        # =====================================================
        # ROW 3: Time-Series Charts
        # =====================================================
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📈 Weekly Sales Over Time")
            time_series = filtered_df.groupby('date')['weekly_sales'].sum().reset_index()
            time_series = time_series.sort_values('date')
            if len(time_series) > 0:
                st.line_chart(data=time_series, x='date', y='weekly_sales', width='stretch')
            else:
                st.info("No data for the selected filters.")

        with col2:
            st.subheader("🤖 Actual vs Predicted Sales")
            comparison = filtered_df.groupby('date').agg(
                Actual=('weekly_sales', 'sum'),
                Predicted=('predicted_sales', 'sum')
            ).reset_index().sort_values('date')
            if len(comparison) > 0:
                st.line_chart(data=comparison, x='date', y=['Actual', 'Predicted'], width='stretch')

        # =====================================================
        # ROW 4: Store-Level Analysis
        # =====================================================
        st.subheader("🏪 Average Sales by Store")
        store_avg = filtered_df.groupby('store')['weekly_sales'].mean().reset_index()
        st.bar_chart(data=store_avg, x='store', y='weekly_sales', width='stretch')

        # =====================================================
        # ROW 5: Seasonal & Festive Sales Analysis
        # =====================================================
        st.markdown("---")
        st.subheader("📅 Seasonal & Festive Sales Analysis")

        filtered_df['Date_Parsed'] = pd.to_datetime(filtered_df['date'], errors='coerce')
        filtered_df['Month'] = filtered_df['Date_Parsed'].dt.month
        filtered_df['Season'] = filtered_df['Month'].apply(get_season)
        filtered_df['Festive_Event'] = filtered_df.apply(get_holiday_name, axis=1)

        col_s1, col_s2 = st.columns(2)

        with col_s1:
            st.write("**Average Sales by Season**")
            season_avg = filtered_df.groupby('Season').agg(
                Actual=('weekly_sales', 'mean'),
                Predicted=('predicted_sales', 'mean')
            ).reset_index()
            season_order = ["Spring", "Summer", "Autumn", "Winter"]
            season_avg['Season'] = pd.Categorical(season_avg['Season'], categories=season_order, ordered=True)
            season_avg = season_avg.sort_values('Season')
            st.bar_chart(data=season_avg, x='Season', y=['Actual', 'Predicted'], width='stretch')

        with col_s2:
            st.write("**Average Sales by Festive Event**")
            festive_avg = filtered_df.groupby('Festive_Event').agg(
                Actual=('weekly_sales', 'mean'),
                Predicted=('predicted_sales', 'mean')
            ).reset_index()
            st.bar_chart(data=festive_avg, x='Festive_Event', y=['Actual', 'Predicted'], width='stretch')

        # =====================================================
        # ROW 6: Top & Bottom Departments
        # =====================================================
        st.markdown("---")
        st.subheader("🏆 Top & Bottom Performing Departments")

        dept_avg = filtered_df.groupby('dept').agg(
            Avg_Sales=('weekly_sales', 'mean'),
            Total_Sales=('weekly_sales', 'sum'),
            Records=('weekly_sales', 'count')
        ).reset_index().sort_values('Avg_Sales', ascending=False)

        col_tb1, col_tb2 = st.columns(2)

        with col_tb1:
            st.write("**🔝 Top 5 Departments by Avg Sales**")
            top5 = dept_avg.head(5).copy()
            top5.columns = ['Department', 'Avg Sales ($)', 'Total Sales ($)', 'Records']
            st.dataframe(
                top5.style.format({
                    'Avg Sales ($)': '${:,.2f}',
                    'Total Sales ($)': '${:,.2f}'
                }),
                width='stretch'
            )

        with col_tb2:
            st.write("**🔻 Bottom 5 Departments by Avg Sales**")
            bottom5 = dept_avg.tail(5).copy()
            bottom5.columns = ['Department', 'Avg Sales ($)', 'Total Sales ($)', 'Records']
            st.dataframe(
                bottom5.style.format({
                    'Avg Sales ($)': '${:,.2f}',
                    'Total Sales ($)': '${:,.2f}'
                }),
                width='stretch'
            )

        # =====================================================
        # ROW 7: Sales Distribution Histogram
        # =====================================================
        st.markdown("---")
        st.subheader("📊 Sales Distribution")

        col_h1, col_h2 = st.columns(2)

        with col_h1:
            st.write("**Weekly Sales Distribution**")
            hist_data = filtered_df[['weekly_sales']].rename(columns={'weekly_sales': 'Weekly Sales ($)'})
            st.bar_chart(
                hist_data['Weekly Sales ($)'].value_counts(bins=20).sort_index(),
                width='stretch'
            )

        with col_h2:
            st.write("**Prediction Error Distribution**")
            filtered_df['Error'] = filtered_df['weekly_sales'] - filtered_df['predicted_sales']
            error_hist = filtered_df[['Error']].rename(columns={'Error': 'Prediction Error ($)'})
            st.bar_chart(
                error_hist['Prediction Error ($)'].value_counts(bins=20).sort_index(),
                width='stretch'
            )

    # =====================================================
    # MongoDB Aggregations Section
    # =====================================================
    if not store_agg.empty:
        st.markdown("---")
        st.subheader("📊 Real-Time Aggregations (from MongoDB)")

        col3, col4 = st.columns(2)
        with col3:
            st.write("**Average Sales by Store**")
            st.bar_chart(data=store_agg, x='Store', y='Avg_Sales_Store', width='stretch')

        with col4:
            if not holiday_agg.empty:
                st.write("**Average Sales: Holiday vs Non-Holiday**")
                holiday_agg['IsHoliday'] = holiday_agg['IsHoliday'].map({True: 'Holiday', False: 'Non-Holiday', 'True': 'Holiday', 'False': 'Non-Holiday'})
                st.bar_chart(data=holiday_agg, x='IsHoliday', y='Avg_Sales_Holiday', width='stretch')

    # =====================================================
    # Predictions Detail Table
    # =====================================================
    if not pg_df.empty:
        st.markdown("---")
        st.subheader("🔍 Latest ML Predictions Detail")
        display_cols = ['store', 'dept', 'date', 'is_holiday', 'weekly_sales', 'predicted_sales']
        table_df = filtered_df[display_cols].copy()
        table_df['Variance ($)'] = table_df['weekly_sales'] - table_df['predicted_sales']

        # Rename columns for a clean presentation
        table_df.columns = ['Store', 'Dept', 'Date', 'Is Holiday', 'Weekly Sales ($)', 'Predicted Sales ($)', 'Variance ($)']

        st.dataframe(
            table_df.style.format({
                'Weekly Sales ($)': '${:,.2f}',
                'Predicted Sales ($)': '${:,.2f}',
                'Variance ($)': '${:,.2f}'
            }),
            width='stretch'
        )

else:
    st.info("⏳ Waiting for data... Ensure Kafka Producer, Stream Processor, and ML Predictor are running.")

# --- Auto-Refresh Logic ---
st.sidebar.markdown("---")
st.sidebar.text("🔄 Auto-refreshing every 30 seconds...")

time.sleep(30)
st.rerun()
