"""
🎯 F&O OPTIONS RISK DASHBOARD - TABLEAU STYLE
=============================================
Interactive dashboard analyzing 50% price reduction patterns in F&O options
Data Sources: Step05_monthly_50percent_reduction_analysis & Step05_strikepriceAnalysisderived
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pyodbc
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="F&O Risk Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Tableau-like styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        border-bottom: 3px solid #1f77b4;
        padding-bottom: 1rem;
    }
    
    .metric-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    
    .metric-label {
        font-size: 0.9rem;
        opacity: 0.9;
    }
    
    .sidebar-section {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        border-left: 4px solid #1f77b4;
    }
    
    .chart-container {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)
def get_database_connection():
    """Establish database connection"""
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=SRIKIRANREDDY\\SQLEXPRESS;'
            'DATABASE=master;'
            'Trusted_Connection=yes;'
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

@st.cache_data(ttl=600)
def load_reduction_analysis_data():
    """Load main reduction analysis data"""
    conn = get_database_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    SELECT 
        symbol,
        strike_price,
        option_type,
        achieved_50_percent_reduction,
        reduction_percentage,
        days_to_50_percent_reduction,
        max_reduction_percentage,
        days_to_max_reduction,
        base_price,
        final_price,
        analysis_date
    FROM Step05_monthly_50percent_reduction_analysis
    """
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading reduction analysis data: {e}")
        conn.close()
        return pd.DataFrame()

@st.cache_data(ttl=600)
def load_strike_analysis_data():
    """Load strike price analysis data"""
    conn = get_database_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    SELECT TOP 10000
        symbol,
        strike_price,
        option_type,
        expiry_date,
        analysis_status,
        first_date,
        last_date
    FROM Step05_strikepriceAnalysisderived
    ORDER BY symbol, strike_price
    """
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading strike analysis data: {e}")
        conn.close()
        return pd.DataFrame()

def create_kpi_metrics(df):
    """Create KPI metrics section"""
    if df.empty:
        return
    
    total_strikes = len(df)
    successful_reductions = len(df[df['achieved_50_percent_reduction'] == 1])
    success_rate = (successful_reductions / total_strikes * 100) if total_strikes > 0 else 0
    avg_days_to_reduction = df[df['achieved_50_percent_reduction'] == 1]['days_to_50_percent_reduction'].mean()
    avg_reduction = df[df['achieved_50_percent_reduction'] == 1]['reduction_percentage'].mean()
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.markdown(f"""
        <div class="metric-container">
            <div class="metric-value">{total_strikes:,}</div>
            <div class="metric-label">Total Strikes Analyzed</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-container">
            <div class="metric-value">{successful_reductions:,}</div>
            <div class="metric-label">Achieved 50%+ Reduction</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-container">
            <div class="metric-value">{success_rate:.1f}%</div>
            <div class="metric-label">Success Rate</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-container">
            <div class="metric-value">{avg_days_to_reduction:.1f}</div>
            <div class="metric-label">Avg Days to 50% Reduction</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col5:
        st.markdown(f"""
        <div class="metric-container">
            <div class="metric-value">{avg_reduction:.1f}%</div>
            <div class="metric-label">Avg Reduction %</div>
        </div>
        """, unsafe_allow_html=True)

def create_symbol_performance_chart(df):
    """Create symbol performance analysis chart"""
    if df.empty:
        return
    
    # Calculate symbol-wise statistics
    symbol_stats = df.groupby('symbol').agg({
        'achieved_50_percent_reduction': ['count', 'sum'],
        'reduction_percentage': 'mean',
        'days_to_50_percent_reduction': 'mean'
    }).round(2)
    
    symbol_stats.columns = ['total_strikes', 'successful_reductions', 'avg_reduction', 'avg_days']
    symbol_stats['success_rate'] = (symbol_stats['successful_reductions'] / symbol_stats['total_strikes'] * 100).round(1)
    symbol_stats = symbol_stats.reset_index()
    
    # Top 20 symbols by success rate
    top_symbols = symbol_stats.nlargest(20, 'success_rate')
    
    fig = px.bar(
        top_symbols,
        x='success_rate',
        y='symbol',
        orientation='h',
        color='avg_reduction',
        color_continuous_scale='RdYlBu_r',
        title="🏆 Top 20 Symbols by 50% Reduction Success Rate",
        labels={
            'success_rate': 'Success Rate (%)',
            'symbol': 'Symbol',
            'avg_reduction': 'Avg Reduction %'
        },
        text='success_rate'
    )
    
    fig.update_traces(texttemplate='%{text:.1f}%', textposition='inside')
    fig.update_layout(
        height=600,
        showlegend=True,
        font=dict(size=12),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    return symbol_stats

def create_reduction_distribution_chart(df):
    """Create reduction percentage distribution chart"""
    if df.empty:
        return
    
    successful_df = df[df['achieved_50_percent_reduction'] == 1]
    
    fig = px.histogram(
        successful_df,
        x='reduction_percentage',
        nbins=30,
        title="📊 Distribution of 50%+ Reduction Percentages",
        labels={
            'reduction_percentage': 'Reduction Percentage (%)',
            'count': 'Number of Strikes'
        },
        color_discrete_sequence=['#1f77b4']
    )
    
    fig.add_vline(
        x=successful_df['reduction_percentage'].mean(),
        line_dash="dash",
        line_color="red",
        annotation_text=f"Mean: {successful_df['reduction_percentage'].mean():.1f}%"
    )
    
    fig.update_layout(
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_time_to_reduction_chart(df):
    """Create time to reduction analysis chart"""
    if df.empty:
        return
    
    successful_df = df[df['achieved_50_percent_reduction'] == 1]
    
    # Days to reduction distribution
    fig1 = px.histogram(
        successful_df,
        x='days_to_50_percent_reduction',
        nbins=20,
        title="⏱️ Days to Achieve 50% Reduction Distribution",
        labels={
            'days_to_50_percent_reduction': 'Days to 50% Reduction',
            'count': 'Number of Strikes'
        },
        color_discrete_sequence=['#ff7f0e']
    )
    
    fig1.add_vline(
        x=successful_df['days_to_50_percent_reduction'].mean(),
        line_dash="dash",
        line_color="red",
        annotation_text=f"Mean: {successful_df['days_to_50_percent_reduction'].mean():.1f} days"
    )
    
    fig1.update_layout(
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    st.plotly_chart(fig1, use_container_width=True)

def create_option_type_analysis(df):
    """Create option type (CE/PE) analysis"""
    if df.empty:
        return
    
    option_stats = df.groupby('option_type').agg({
        'achieved_50_percent_reduction': ['count', 'sum'],
        'reduction_percentage': 'mean',
        'days_to_50_percent_reduction': 'mean'
    }).round(2)
    
    option_stats.columns = ['total_strikes', 'successful_reductions', 'avg_reduction', 'avg_days']
    option_stats['success_rate'] = (option_stats['successful_reductions'] / option_stats['total_strikes'] * 100).round(1)
    option_stats = option_stats.reset_index()
    
    # Create subplot with 2 charts
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Success Rate by Option Type', 'Average Reduction by Option Type'),
        specs=[[{"type": "bar"}, {"type": "bar"}]]
    )
    
    # Success rate chart
    fig.add_trace(
        go.Bar(
            x=option_stats['option_type'],
            y=option_stats['success_rate'],
            name='Success Rate (%)',
            marker_color=['#1f77b4', '#ff7f0e'],
            text=option_stats['success_rate'],
            texttemplate='%{text:.1f}%'
        ),
        row=1, col=1
    )
    
    # Average reduction chart
    fig.add_trace(
        go.Bar(
            x=option_stats['option_type'],
            y=option_stats['avg_reduction'],
            name='Avg Reduction (%)',
            marker_color=['#2ca02c', '#d62728'],
            text=option_stats['avg_reduction'],
            texttemplate='%{text:.1f}%'
        ),
        row=1, col=2
    )
    
    fig.update_layout(
        title_text="📈 Call vs Put Options Analysis",
        height=400,
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    return option_stats

def create_risk_heatmap(df):
    """Create risk heatmap by symbol and option type"""
    if df.empty:
        return
    
    # Calculate success rate by symbol and option type
    heatmap_data = df.groupby(['symbol', 'option_type']).agg({
        'achieved_50_percent_reduction': ['count', 'sum']
    }).round(2)
    
    heatmap_data.columns = ['total', 'successful']
    heatmap_data['success_rate'] = (heatmap_data['successful'] / heatmap_data['total'] * 100).round(1)
    heatmap_data = heatmap_data.reset_index()
    
    # Pivot for heatmap
    heatmap_pivot = heatmap_data.pivot(index='symbol', columns='option_type', values='success_rate').fillna(0)
    
    # Select top 30 symbols for readability
    top_symbols = df.groupby('symbol')['achieved_50_percent_reduction'].sum().nlargest(30).index
    heatmap_pivot = heatmap_pivot.loc[heatmap_pivot.index.isin(top_symbols)]
    
    fig = px.imshow(
        heatmap_pivot.values,
        x=heatmap_pivot.columns,
        y=heatmap_pivot.index,
        color_continuous_scale='RdYlBu_r',
        title="🔥 Risk Heatmap: Success Rate by Symbol & Option Type (Top 30 Symbols)",
        labels={'color': 'Success Rate (%)'}
    )
    
    fig.update_layout(
        height=800,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_detailed_data_table(df, symbol_stats):
    """Create detailed data table with filters"""
    st.markdown("### 📋 Detailed Analysis Data")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        selected_symbols = st.multiselect(
            "Select Symbols",
            options=sorted(df['symbol'].unique()),
            default=[]
        )
    
    with col2:
        option_types = st.multiselect(
            "Select Option Types",
            options=['CE', 'PE'],
            default=['CE', 'PE']
        )
    
    with col3:
        reduction_filter = st.selectbox(
            "Filter by Reduction Achievement",
            options=['All', 'Achieved 50%+', 'Did not achieve 50%+']
        )
    
    # Apply filters
    filtered_df = df.copy()
    
    if selected_symbols:
        filtered_df = filtered_df[filtered_df['symbol'].isin(selected_symbols)]
    
    if option_types:
        filtered_df = filtered_df[filtered_df['option_type'].isin(option_types)]
    
    if reduction_filter == 'Achieved 50%+':
        filtered_df = filtered_df[filtered_df['achieved_50_percent_reduction'] == 1]
    elif reduction_filter == 'Did not achieve 50%+':
        filtered_df = filtered_df[filtered_df['achieved_50_percent_reduction'] == 0]
    
    # Display filtered data
    st.dataframe(
        filtered_df.style.format({
            'reduction_percentage': '{:.2f}%',
            'max_reduction_percentage': '{:.2f}%',
            'base_price': '₹{:.2f}',
            'final_price': '₹{:.2f}'
        }),
        use_container_width=True,
        height=400
    )
    
    # Summary stats for filtered data
    if not filtered_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Filtered Records", f"{len(filtered_df):,}")
        
        with col2:
            success_count = len(filtered_df[filtered_df['achieved_50_percent_reduction'] == 1])
            st.metric("50%+ Achieved", f"{success_count:,}")
        
        with col3:
            success_rate = (success_count / len(filtered_df) * 100) if len(filtered_df) > 0 else 0
            st.metric("Success Rate", f"{success_rate:.1f}%")
        
        with col4:
            if success_count > 0:
                avg_days = filtered_df[filtered_df['achieved_50_percent_reduction'] == 1]['days_to_50_percent_reduction'].mean()
                st.metric("Avg Days", f"{avg_days:.1f}")

def main():
    """Main dashboard function"""
    
    # Header
    st.markdown('<h1 class="main-header">🎯 F&O OPTIONS RISK ANALYTICS DASHBOARD</h1>', unsafe_allow_html=True)
    st.markdown("*Comprehensive analysis of 50% price reduction patterns in F&O options trading*")
    
    # Sidebar
    with st.sidebar:
        st.markdown("### 🎛️ Dashboard Controls")
        
        # Refresh data button
        if st.button("🔄 Refresh Data", type="primary"):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("### 📊 Data Sources")
        st.info("**Primary:** Step05_monthly_50percent_reduction_analysis")
        st.info("**Secondary:** Step05_strikepriceAnalysisderived")
        
        st.markdown("### ℹ️ About This Dashboard")
        st.markdown("""
        This dashboard provides comprehensive analytics on F&O options that experienced 
        50%+ price reductions during February 2025.
        
        **Key Metrics:**
        - Success rates by symbol
        - Time to reduction analysis
        - Option type comparisons
        - Risk heatmaps
        """)
    
    # Load data
    with st.spinner("🔄 Loading data from database..."):
        reduction_df = load_reduction_analysis_data()
        strike_df = load_strike_analysis_data()
    
    if reduction_df.empty:
        st.error("❌ No data available. Please check database connection.")
        return
    
    # KPI Metrics
    st.markdown("## 📊 Key Performance Indicators")
    create_kpi_metrics(reduction_df)
    
    # Main analysis section
    st.markdown("## 📈 Performance Analysis")
    
    # Create two columns for charts
    col1, col2 = st.columns(2)
    
    with col1:
        with st.container():
            create_reduction_distribution_chart(reduction_df)
    
    with col2:
        with st.container():
            create_time_to_reduction_chart(reduction_df)
    
    # Symbol performance
    st.markdown("## 🏆 Symbol Performance Analysis")
    symbol_stats = create_symbol_performance_chart(reduction_df)
    
    # Option type analysis
    st.markdown("## 📊 Call vs Put Analysis")
    option_stats = create_option_type_analysis(reduction_df)
    
    # Risk heatmap
    st.markdown("## 🔥 Risk Heatmap")
    create_risk_heatmap(reduction_df)
    
    # Detailed data
    if symbol_stats is not None:
        create_detailed_data_table(reduction_df, symbol_stats)
    
    # Footer
    st.markdown("---")
    st.markdown("*Dashboard created for F&O options risk analysis - Data as of February 2025*")

if __name__ == "__main__":
    main()