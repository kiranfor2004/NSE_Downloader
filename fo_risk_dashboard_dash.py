"""
🎯 F&O OPTIONS RISK DASHBOARD - PLOTLY DASH VERSION
==================================================
Alternative Tableau-style dashboard using Plotly Dash
Data Sources: Step05_monthly_50percent_reduction_analysis & Step05_strikepriceAnalysisderived
"""

import dash
from dash import dcc, html, dash_table, Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import pyodbc
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Initialize Dash app
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "F&O Risk Analytics Dashboard"

# Custom CSS styling
external_stylesheets = [
    'https://codepen.io/chriddyp/pen/bWLwgP.css',
    'https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;700&display=swap'
]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# Database connection function
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
        print(f"Database connection failed: {e}")
        return None

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
        print(f"Error loading reduction analysis data: {e}")
        conn.close()
        return pd.DataFrame()

# Load data
print("📊 Loading data from database...")
df = load_reduction_analysis_data()

if df.empty:
    print("❌ No data available. Please check database connection.")
else:
    print(f"✅ Loaded {len(df):,} records successfully")

# Calculate KPIs
def calculate_kpis(df):
    """Calculate KPI metrics"""
    if df.empty:
        return {}
    
    total_strikes = len(df)
    successful_reductions = len(df[df['achieved_50_percent_reduction'] == 1])
    success_rate = (successful_reductions / total_strikes * 100) if total_strikes > 0 else 0
    avg_days_to_reduction = df[df['achieved_50_percent_reduction'] == 1]['days_to_50_percent_reduction'].mean()
    avg_reduction = df[df['achieved_50_percent_reduction'] == 1]['reduction_percentage'].mean()
    
    return {
        'total_strikes': total_strikes,
        'successful_reductions': successful_reductions,
        'success_rate': success_rate,
        'avg_days_to_reduction': avg_days_to_reduction,
        'avg_reduction': avg_reduction
    }

kpis = calculate_kpis(df)

# Create symbol performance data
def create_symbol_stats(df):
    """Create symbol performance statistics"""
    if df.empty:
        return pd.DataFrame()
    
    symbol_stats = df.groupby('symbol').agg({
        'achieved_50_percent_reduction': ['count', 'sum'],
        'reduction_percentage': 'mean',
        'days_to_50_percent_reduction': 'mean'
    }).round(2)
    
    symbol_stats.columns = ['total_strikes', 'successful_reductions', 'avg_reduction', 'avg_days']
    symbol_stats['success_rate'] = (symbol_stats['successful_reductions'] / symbol_stats['total_strikes'] * 100).round(1)
    symbol_stats = symbol_stats.reset_index()
    
    return symbol_stats.sort_values('success_rate', ascending=False)

symbol_stats = create_symbol_stats(df)

# App layout
app.layout = html.Div([
    # Header
    html.Div([
        html.H1("🎯 F&O OPTIONS RISK ANALYTICS DASHBOARD", 
                style={
                    'textAlign': 'center',
                    'color': '#1f77b4',
                    'fontFamily': 'Roboto',
                    'fontSize': '2.5rem',
                    'fontWeight': 'bold',
                    'marginBottom': '20px',
                    'borderBottom': '3px solid #1f77b4',
                    'paddingBottom': '20px'
                }),
        html.P("Interactive analysis of 50% price reduction patterns in F&O options trading",
               style={
                   'textAlign': 'center',
                   'fontSize': '1.2rem',
                   'color': '#666',
                   'fontStyle': 'italic',
                   'marginBottom': '30px'
               })
    ]),
    
    # KPI Cards
    html.Div([
        html.H2("📊 Key Performance Indicators", style={'color': '#1f77b4', 'marginBottom': '20px'}),
        html.Div([
            html.Div([
                html.H3(f"{kpis.get('total_strikes', 0):,}", style={'fontSize': '2rem', 'margin': '0', 'color': 'white'}),
                html.P("Total Strikes Analyzed", style={'margin': '0', 'color': 'white', 'opacity': '0.9'})
            ], style={
                'background': 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                'padding': '20px',
                'borderRadius': '10px',
                'textAlign': 'center',
                'margin': '10px'
            }, className='three columns'),
            
            html.Div([
                html.H3(f"{kpis.get('successful_reductions', 0):,}", style={'fontSize': '2rem', 'margin': '0', 'color': 'white'}),
                html.P("Achieved 50%+ Reduction", style={'margin': '0', 'color': 'white', 'opacity': '0.9'})
            ], style={
                'background': 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                'padding': '20px',
                'borderRadius': '10px',
                'textAlign': 'center',
                'margin': '10px'
            }, className='three columns'),
            
            html.Div([
                html.H3(f"{kpis.get('success_rate', 0):.1f}%", style={'fontSize': '2rem', 'margin': '0', 'color': 'white'}),
                html.P("Success Rate", style={'margin': '0', 'color': 'white', 'opacity': '0.9'})
            ], style={
                'background': 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                'padding': '20px',
                'borderRadius': '10px',
                'textAlign': 'center',
                'margin': '10px'
            }, className='three columns'),
            
            html.Div([
                html.H3(f"{kpis.get('avg_days_to_reduction', 0):.1f}", style={'fontSize': '2rem', 'margin': '0', 'color': 'white'}),
                html.P("Avg Days to 50% Reduction", style={'margin': '0', 'color': 'white', 'opacity': '0.9'})
            ], style={
                'background': 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                'padding': '20px',
                'borderRadius': '10px',
                'textAlign': 'center',
                'margin': '10px'
            }, className='three columns'),
        ], className='row'),
    ], style={'marginBottom': '40px'}),
    
    # Charts Section
    html.Div([
        html.H2("📈 Performance Analysis", style={'color': '#1f77b4', 'marginBottom': '20px'}),
        
        # Top row charts
        html.Div([
            html.Div([
                dcc.Graph(id='reduction-distribution')
            ], className='six columns'),
            
            html.Div([
                dcc.Graph(id='time-to-reduction')
            ], className='six columns'),
        ], className='row'),
        
        # Symbol performance chart
        html.Div([
            dcc.Graph(id='symbol-performance')
        ], style={'marginTop': '20px'}),
        
        # Option type analysis
        html.Div([
            dcc.Graph(id='option-type-analysis')
        ], style={'marginTop': '20px'}),
        
        # Risk heatmap
        html.Div([
            dcc.Graph(id='risk-heatmap')
        ], style={'marginTop': '20px'}),
        
    ]),
    
    # Data Table Section
    html.Div([
        html.H2("📋 Detailed Analysis Data", style={'color': '#1f77b4', 'marginBottom': '20px'}),
        
        # Filters
        html.Div([
            html.Div([
                html.Label("Select Symbols:"),
                dcc.Dropdown(
                    id='symbol-filter',
                    options=[{'label': symbol, 'value': symbol} for symbol in sorted(df['symbol'].unique())] if not df.empty else [],
                    value=[],
                    multi=True,
                    placeholder="Select symbols..."
                )
            ], className='four columns'),
            
            html.Div([
                html.Label("Select Option Types:"),
                dcc.Dropdown(
                    id='option-type-filter',
                    options=[
                        {'label': 'Call (CE)', 'value': 'CE'},
                        {'label': 'Put (PE)', 'value': 'PE'}
                    ],
                    value=['CE', 'PE'],
                    multi=True
                )
            ], className='four columns'),
            
            html.Div([
                html.Label("Filter by Achievement:"),
                dcc.Dropdown(
                    id='achievement-filter',
                    options=[
                        {'label': 'All', 'value': 'all'},
                        {'label': 'Achieved 50%+', 'value': 'achieved'},
                        {'label': 'Did not achieve 50%+', 'value': 'not_achieved'}
                    ],
                    value='all'
                )
            ], className='four columns'),
        ], className='row', style={'marginBottom': '20px'}),
        
        # Data table
        html.Div(id='data-table-container')
        
    ], style={'marginTop': '40px'}),
    
    # Footer
    html.Hr(),
    html.Div([
        html.P("Dashboard created for F&O options risk analysis - Data as of February 2025",
               style={'textAlign': 'center', 'fontStyle': 'italic', 'color': '#666'})
    ])
    
], style={'margin': '20px', 'fontFamily': 'Roboto'})

# Callbacks for charts
@app.callback(
    Output('reduction-distribution', 'figure'),
    Input('reduction-distribution', 'id')
)
def update_reduction_distribution(_):
    if df.empty:
        return go.Figure()
    
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
    
    return fig

@app.callback(
    Output('time-to-reduction', 'figure'),
    Input('time-to-reduction', 'id')
)
def update_time_to_reduction(_):
    if df.empty:
        return go.Figure()
    
    successful_df = df[df['achieved_50_percent_reduction'] == 1]
    
    fig = px.histogram(
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
    
    fig.add_vline(
        x=successful_df['days_to_50_percent_reduction'].mean(),
        line_dash="dash",
        line_color="red",
        annotation_text=f"Mean: {successful_df['days_to_50_percent_reduction'].mean():.1f} days"
    )
    
    fig.update_layout(
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

@app.callback(
    Output('symbol-performance', 'figure'),
    Input('symbol-performance', 'id')
)
def update_symbol_performance(_):
    if symbol_stats.empty:
        return go.Figure()
    
    top_symbols = symbol_stats.head(20)
    
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
    
    return fig

@app.callback(
    Output('option-type-analysis', 'figure'),
    Input('option-type-analysis', 'id')
)
def update_option_type_analysis(_):
    if df.empty:
        return go.Figure()
    
    option_stats = df.groupby('option_type').agg({
        'achieved_50_percent_reduction': ['count', 'sum'],
        'reduction_percentage': 'mean',
        'days_to_50_percent_reduction': 'mean'
    }).round(2)
    
    option_stats.columns = ['total_strikes', 'successful_reductions', 'avg_reduction', 'avg_days']
    option_stats['success_rate'] = (option_stats['successful_reductions'] / option_stats['total_strikes'] * 100).round(1)
    option_stats = option_stats.reset_index()
    
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Success Rate by Option Type', 'Average Reduction by Option Type'),
        specs=[[{"type": "bar"}, {"type": "bar"}]]
    )
    
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
    
    return fig

@app.callback(
    Output('risk-heatmap', 'figure'),
    Input('risk-heatmap', 'id')
)
def update_risk_heatmap(_):
    if df.empty:
        return go.Figure()
    
    heatmap_data = df.groupby(['symbol', 'option_type']).agg({
        'achieved_50_percent_reduction': ['count', 'sum']
    }).round(2)
    
    heatmap_data.columns = ['total', 'successful']
    heatmap_data['success_rate'] = (heatmap_data['successful'] / heatmap_data['total'] * 100).round(1)
    heatmap_data = heatmap_data.reset_index()
    
    heatmap_pivot = heatmap_data.pivot(index='symbol', columns='option_type', values='success_rate').fillna(0)
    
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
    
    return fig

@app.callback(
    Output('data-table-container', 'children'),
    [Input('symbol-filter', 'value'),
     Input('option-type-filter', 'value'),
     Input('achievement-filter', 'value')]
)
def update_data_table(selected_symbols, option_types, achievement_filter):
    if df.empty:
        return html.Div("No data available")
    
    filtered_df = df.copy()
    
    if selected_symbols:
        filtered_df = filtered_df[filtered_df['symbol'].isin(selected_symbols)]
    
    if option_types:
        filtered_df = filtered_df[filtered_df['option_type'].isin(option_types)]
    
    if achievement_filter == 'achieved':
        filtered_df = filtered_df[filtered_df['achieved_50_percent_reduction'] == 1]
    elif achievement_filter == 'not_achieved':
        filtered_df = filtered_df[filtered_df['achieved_50_percent_reduction'] == 0]
    
    # Summary stats
    success_count = len(filtered_df[filtered_df['achieved_50_percent_reduction'] == 1])
    success_rate = (success_count / len(filtered_df) * 100) if len(filtered_df) > 0 else 0
    
    summary_cards = html.Div([
        html.Div([
            html.H4(f"{len(filtered_df):,}", style={'margin': '0', 'color': '#1f77b4'}),
            html.P("Filtered Records", style={'margin': '0'})
        ], className='three columns', style={'textAlign': 'center', 'padding': '10px', 'background': '#f8f9fa', 'borderRadius': '5px', 'margin': '5px'}),
        
        html.Div([
            html.H4(f"{success_count:,}", style={'margin': '0', 'color': '#1f77b4'}),
            html.P("50%+ Achieved", style={'margin': '0'})
        ], className='three columns', style={'textAlign': 'center', 'padding': '10px', 'background': '#f8f9fa', 'borderRadius': '5px', 'margin': '5px'}),
        
        html.Div([
            html.H4(f"{success_rate:.1f}%", style={'margin': '0', 'color': '#1f77b4'}),
            html.P("Success Rate", style={'margin': '0'})
        ], className='three columns', style={'textAlign': 'center', 'padding': '10px', 'background': '#f8f9fa', 'borderRadius': '5px', 'margin': '5px'}),
        
        html.Div([
            html.H4(f"{filtered_df[filtered_df['achieved_50_percent_reduction'] == 1]['days_to_50_percent_reduction'].mean():.1f}" if success_count > 0 else "N/A", 
                    style={'margin': '0', 'color': '#1f77b4'}),
            html.P("Avg Days", style={'margin': '0'})
        ], className='three columns', style={'textAlign': 'center', 'padding': '10px', 'background': '#f8f9fa', 'borderRadius': '5px', 'margin': '5px'}),
    ], className='row', style={'marginBottom': '20px'})
    
    # Data table
    data_table = dash_table.DataTable(
        data=filtered_df.head(1000).to_dict('records'),
        columns=[
            {'name': 'Symbol', 'id': 'symbol'},
            {'name': 'Strike', 'id': 'strike_price', 'type': 'numeric'},
            {'name': 'Type', 'id': 'option_type'},
            {'name': '50%+ Achieved', 'id': 'achieved_50_percent_reduction'},
            {'name': 'Reduction %', 'id': 'reduction_percentage', 'type': 'numeric', 'format': {'specifier': '.2f'}},
            {'name': 'Days to 50%', 'id': 'days_to_50_percent_reduction', 'type': 'numeric'},
            {'name': 'Max Reduction %', 'id': 'max_reduction_percentage', 'type': 'numeric', 'format': {'specifier': '.2f'}},
            {'name': 'Base Price', 'id': 'base_price', 'type': 'numeric', 'format': {'specifier': '.2f'}},
            {'name': 'Final Price', 'id': 'final_price', 'type': 'numeric', 'format': {'specifier': '.2f'}},
        ],
        style_table={'height': '400px', 'overflowY': 'auto'},
        style_cell={'textAlign': 'center', 'fontSize': '12px'},
        style_header={'backgroundColor': '#1f77b4', 'color': 'white', 'fontWeight': 'bold'},
        style_data_conditional=[
            {
                'if': {'filter_query': '{achieved_50_percent_reduction} = 1'},
                'backgroundColor': '#d4edda',
                'color': 'black',
            },
            {
                'if': {'filter_query': '{achieved_50_percent_reduction} = 0'},
                'backgroundColor': '#f8d7da',
                'color': 'black',
            }
        ],
        sort_action="native",
        filter_action="native",
        page_action="native",
        page_current=0,
        page_size=50,
    )
    
    return html.Div([summary_cards, data_table])

if __name__ == '__main__':
    print("🚀 Starting F&O Risk Analytics Dashboard...")
    print("📊 Tableau-style interface loading...")
    print("🌐 Dashboard will be available at: http://localhost:8050")
    print("\n" + "="*60)
    print("🎯 F&O OPTIONS RISK ANALYTICS DASHBOARD")
    print("📈 Interactive Analysis of 50% Price Reductions")
    print("📊 Data: Step05_monthly_50percent_reduction_analysis")
    print("="*60 + "\n")
    
    app.run_server(debug=True, host='0.0.0.0', port=8050)