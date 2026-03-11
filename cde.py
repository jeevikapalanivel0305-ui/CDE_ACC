"""
CDE Risk Assessment Platform
Critical Data Element Governance Platform
Powered by iLink Digital
With AI-Powered Action Suggestions
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
import json
import io
import sys
import os
from dotenv import load_dotenv

# Standardized .env loading
from pathlib import Path
dotenv_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=dotenv_path)

# Add current directory to path to find backend modules if needed
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from google import genai
from backend.purview_connector import PurviewConnector
from backend.fabric_connector import FabricConnector
from backend.ai_recommender import render_ai_recommend, generate_cde_suggestions, get_ai_client
import requests
import time
import base64
# ============================================
# PAGE CONFIGURATION
# ============================================
st.set_page_config(
    page_title="CDE Catalyst | iLink Digital",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# CUSTOM CSS STYLING
# ============================================
# Updated CSS for hamburger menu icon - add this to your Purv.py CSS section


def load_css(file_name):
    """Load and inject CSS styling"""
    # Use path relative to this script
    css_path = os.path.join(os.path.dirname(__file__), file_name)
    try:
        with open(css_path) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except FileNotFoundError:
        st.warning(f"CSS file not found: {css_path}")

def inject_tab_icons():
    """Empty - icons removed as per user request"""
    pass

# Load custom styling
load_css('style.css')
# ============================================
# CONSTANTS
# ============================================
inject_tab_icons()
# ============================================
DOMAINS = ['General', 'Finance / Banking', 'Healthcare', 'Retail / E-Commerce', 'Manufacturing', 'Energy / Utilities', 'Government', 'Insurance']
ACTION_TYPES = ['Security', 'Quality', 'Control', 'Governance', 'Documentation', 'Training']
PRIORITIES = ['P1', 'P2', 'P3', 'P4']
ACTION_STATUSES = ['Not Started', 'In Progress', 'On Hold', 'Complete', 'Cancelled']

CRITERIA = [
    {"id": 1, "name": "Regulatory Requirement", "desc": "Required for regulatory reporting"},
    {"id": 2, "name": "Financial Materiality", "desc": "Impacts financial statements"},
    {"id": 3, "name": "Risk Management", "desc": "Used in risk models"},
    {"id": 4, "name": "Customer-Facing", "desc": "Appears on communications"},
    {"id": 5, "name": "Cross-System Dependency", "desc": "3+ downstream systems"},
    {"id": 6, "name": "Executive Reporting", "desc": "In board reports"},
    {"id": 7, "name": "Legal/Contractual", "desc": "In legal agreements"},
    {"id": 8, "name": "Audit Trail", "desc": "Subject to audit"},
    {"id": 9, "name": "Unique Identifier", "desc": "Primary key across systems"},
    {"id": 10, "name": "Sensitive/PII", "desc": "Personal/confidential info"},
]

# Initial sample data
INITIAL_CDES = [
    {"id": "CDE-001", "name": "Customer SSN", "domain": "Customer", "definition": "Social Security Number for customer identification", "dataType": "String(9)", "sourceSystem": "Excel", "steward": "John Smith", "owner": "Jane Doe", "downstreamSystems": "CRM, Reporting, Risk", "regulatory": "GLBA, IRS", "businessImpact": 5, "regulatoryCompliance": 5, "dataQualityRisk": 4, "securityRisk": 5, "systemComplexity": 4, "recoveryDifficulty": 5, "status": "Active", "assessmentDate": "2024-01-15", "notes": "PII - Highest sensitivity"},
    {"id": "CDE-002", "name": "Account Balance", "domain": "Account", "definition": "Current balance of customer account", "dataType": "Decimal(15,2)", "sourceSystem": "Excel", "steward": "Mary Johnson", "owner": "Jane Doe", "downstreamSystems": "Reporting, Mobile, Web", "regulatory": "SOX, Basel", "businessImpact": 5, "regulatoryCompliance": 5, "dataQualityRisk": 3, "securityRisk": 4, "systemComplexity": 4, "recoveryDifficulty": 3, "status": "Active", "assessmentDate": "2024-01-15", "notes": "Real-time calculation"},
    {"id": "CDE-003", "name": "Customer Name", "domain": "Customer", "definition": "Full legal name of customer", "dataType": "String(100)", "sourceSystem": "Excel", "steward": "John Smith", "owner": "Jane Doe", "downstreamSystems": "CRM, Statements, Compliance", "regulatory": "KYC, AML", "businessImpact": 4, "regulatoryCompliance": 4, "dataQualityRisk": 3, "securityRisk": 4, "systemComplexity": 3, "recoveryDifficulty": 2, "status": "Active", "assessmentDate": "2024-01-20", "notes": ""},
    {"id": "CDE-004", "name": "Transaction Amount", "domain": "Transaction", "definition": "Monetary value of transaction", "dataType": "Decimal(15,2)", "sourceSystem": "Excel", "steward": "Bob Williams", "owner": "Mike Brown", "downstreamSystems": "Reporting, Analytics, Fraud", "regulatory": "SOX, AML", "businessImpact": 5, "regulatoryCompliance": 5, "dataQualityRisk": 3, "securityRisk": 3, "systemComplexity": 4, "recoveryDifficulty": 3, "status": "Active", "assessmentDate": "2024-02-01", "notes": ""},
    {"id": "CDE-005", "name": "Credit Score", "domain": "Risk", "definition": "Customer credit risk score", "dataType": "Integer", "sourceSystem": "Excel", "steward": "Sarah Davis", "owner": "Mike Brown", "downstreamSystems": "Lending, Risk, Reporting", "regulatory": "Basel, CECL", "businessImpact": 4, "regulatoryCompliance": 4, "dataQualityRisk": 4, "securityRisk": 3, "systemComplexity": 3, "recoveryDifficulty": 3, "status": "Active", "assessmentDate": "2024-02-10", "notes": "External vendor data"},
]

INITIAL_ACTIONS = [
    {"id": "ACT-001", "cdeId": "CDE-001", "cdeName": "Customer SSN", "riskTier": "Critical", "description": "Implement encryption at rest for all SSN storage\nAdd field-level encryption in database\nUpdate backup encryption protocols", "type": "Security", "priority": "P1", "owner": "IT Security", "dueDate": "2024-03-31", "status": "In Progress", "percentComplete": 60, "notes": "Vendor selected"},
    {"id": "ACT-002", "cdeId": "CDE-001", "cdeName": "Customer SSN", "riskTier": "Critical", "description": "Add data masking for non-production environments\nImplement dynamic masking rules\nTest with QA team", "type": "Security", "priority": "P1", "owner": "Data Team", "dueDate": "2024-02-28", "status": "Complete", "percentComplete": 100, "notes": ""},
    {"id": "ACT-003", "cdeId": "CDE-002", "cdeName": "Account Balance", "riskTier": "Critical", "description": "Implement real-time monitoring dashboard\nSet up alerts for balance discrepancies\nEstablish reconciliation procedures", "type": "Quality", "priority": "P1", "owner": "Operations", "dueDate": "2024-04-15", "status": "Not Started", "percentComplete": 0, "notes": "Budget approved"},
]

# ============================================
# LLM INTEGRATION
# ============================================


def generate_action_suggestions(action_name, cde_info):
    """Generate action suggestions and priority using AI"""
    client = get_ai_client()
    if not client:
        return {
            "description": "Please enter action description manually",
            "priority": "P2"
        }
    
    risk_score = calculate_weighted_score(cde_info)
    risk_tier = get_risk_tier(risk_score)
    
    prompt = f"""You are a data governance expert helping to create actionable remediation plans for Critical Data Elements (CDEs).

Based on the following CDE information, generate specific, actionable recommendations:

**Action/CDE Name:** {action_name}

**CDE Details:**
- Domain: {cde_info.get('domain', 'N/A')}
- Risk Tier: {risk_tier}
- Risk Score: {risk_score}/5.0
- Business Impact: {cde_info.get('businessImpact', 'N/A')}/5
- Regulatory Compliance: {cde_info.get('regulatoryCompliance', 'N/A')}/5
- Data Quality Risk: {cde_info.get('dataQualityRisk', 'N/A')}/5
- Security Risk: {cde_info.get('securityRisk', 'N/A')}/5
- System Complexity: {cde_info.get('systemComplexity', 'N/A')}/5
- Source System: {cde_info.get('sourceSystem', 'N/A')}
- Regulatory Requirements: {cde_info.get('regulatory', 'N/A')}

**Task:**
Generate 2-3 specific, actionable recommendations for "{action_name}" that address the identified risks. Each recommendation should be on a new line. Focus on practical steps that can be implemented.

Also determine the priority level:
- **P1 (Critical)**: Risk Score >= 4.0, immediate action required
- **P2 (High)**: Risk Score 3.0-3.99, action needed within 48 hours
- **P3 (Medium)**: Risk Score 2.0-2.99, action within 1 week
- **P4 (Low)**: Risk Score < 2.0, standard timeline

Respond ONLY in this JSON format (no markdown, no code blocks):
{{
    "description": "Detailed action suggestions (2-3 specific items, each on new line)",
    "priority": "P1 or P2 or P3 or P4"
}}"""

    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt
        )
        
        response_text = response.text
        response_text = response_text.replace("```json", "").replace("```", "").strip()
        result = json.loads(response_text)
        return result
    
    except Exception as e:
        st.error(f"Error generating suggestions: {str(e)}")
        return {
            "description": "Please enter action description manually",
            "priority": "P2"
        }

# ============================================
# UTILITY FUNCTIONS
# ============================================
def calculate_weighted_score(cde):
    """Calculate weighted risk score for a CDE"""
    score = (
        cde.get('businessImpact', 3) * 0.25 +
        cde.get('regulatoryCompliance', 3) * 0.20 +
        cde.get('dataQualityRisk', 3) * 0.20 +
        cde.get('securityRisk', 3) * 0.15 +
        cde.get('systemComplexity', 3) * 0.10 +
        cde.get('recoveryDifficulty', 3) * 0.10
    )
    return round(score, 2)

def get_risk_tier(score):
    """Get risk tier based on score"""
    if score >= 4:
        return 'Critical'
    elif score >= 3:
        return 'High'
    elif score >= 2:
        return 'Medium'
    return 'Low'

def get_risk_color(tier):
    """Get color for risk tier"""
    colors = {
        'Critical': '#C00000',
        'High': '#ED7D31',
        'Medium': '#FFC000',
        'Low': '#70AD47'
    }
    return colors.get(tier, '#666666')

def get_risk_bg(tier):
    """Get background color for risk tier"""
    colors = {
        'Critical': '#FEE2E2',
        'High': '#FED7AA',
        'Medium': '#FEF3C7',
        'Low': '#D1FAE5'
    }
    return colors.get(tier, '#F3F4F6')

def render_risk_badge(tier):
    """Render HTML risk badge"""
    color = get_risk_color(tier)
    return f'<span class="risk-badge" style="background-color: {color};">{tier}</span>'

def render_priority_badge(priority):
    """Render HTML priority badge"""
    return f'<span class="priority-{priority.lower()}">{priority}</span>'

def render_status_badge(status):
    """Render HTML status badge"""
    status_class = 'complete' if status == 'Complete' else 'progress' if status == 'In Progress' else 'notstarted'
    return f'<span class="status-{status_class}">{status}</span>'
def find_column_index(columns, possible_names):
    """Find the index of a column by checking possible names"""
    for i, col in enumerate(columns):
        if col and col.lower().strip() in [n.lower() for n in possible_names]:
            return i
    return 0

def get_score_value(row, col, use_default=True):
    """Get a score value from a row, with validation"""
    if not col:
        return 3 if use_default else 1
    
    try:
        val = row.get(col)
        if pd.isna(val):
            return 3 if use_default else 1
        val = int(float(val))
        return max(1, min(5, val))  # Clamp between 1-5
    except:
        return 3 if use_default else 1
# ============================================
# SESSION STATE INITIALIZATION
# ============================================

def init_session_state():
    """Initialize session state - ONLY on first run, NEVER overwrite"""
    
    # Ensure persistent storage for connectors exists
    if 'connector_creds' not in st.session_state:
        st.session_state.connector_creds = {
            'purview_account_name': '', 'purview_tenant_id': '', 'purview_client_id': '', 'purview_client_secret': '',
            'fabric_tenant_id': '', 'fabric_client_id': '', 'fabric_client_secret': '',
            'fabric_sql_endpoint': '', 'fabric_table_name': ''
        }
    
    # Initialize persistent background storage for onboard sub-tabs and forms
    if 'onboard_sub_tab' not in st.session_state: st.session_state.onboard_sub_tab = "Add CDE"
    if 'cde_active_tab' not in st.session_state: st.session_state.cde_active_tab = 0
    if 'onboard_form_data' not in st.session_state:
        st.session_state.onboard_form_data = {
            'add_name': '', 'add_domain': DOMAINS[0], 'add_source': '', 'add_steward': '', 
            'add_owner': '', 'add_def': '', 'eval_selected_cde': None,
            'eval_checklist': {}
        }
        
    # Use a flag to track if we've done initial setup
    if 'app_initialized' not in st.session_state:
        st.session_state.app_initialized = True
        st.session_state.cdes = INITIAL_CDES.copy()
        st.session_state.actions = INITIAL_ACTIONS.copy()
        st.session_state.checklist = {str(i): False for i in range(1, 11)}
        st.session_state.checklist_name = ""
        st.session_state.llm_description = ""
        st.session_state.llm_priority = "P2"
        st.session_state.show_cde_form = False
        st.session_state.editing_cde_id = None
        st.session_state.show_action_form = False
        st.session_state.editing_action_id = None
        st.session_state.purview_import_successful = False
        st.session_state.last_import_count = 0
        st.session_state.switch_to_view_tab = False
        st.session_state.candidate_queue = [] # Queue for CDE Onboarding
        st.session_state.fabric_import_successful = False
        st.session_state.fabric_cdes = []
        st.session_state.fabric_tables = []
        
        # Initialize connector credentials for persistence
        if 'fabric_client_secret' not in st.session_state: st.session_state.fabric_client_secret = ""
        
        # Log first initialization
        print("🔧 FIRST INITIALIZATION - Setting up initial CDEs")
    else:
        # Already initialized - do NOTHING to st.session_state.cdes
        print(f"♻️ APP RERUN - Preserving existing {len(st.session_state.cdes)} CDEs")

# ============================================
# HEADER
# ============================================
def render_header():
    """Render the accelerator-style header matching the Data Quality Analyser format"""
    import base64
    import os
    
    # Header background style
    st.markdown("""
        <style>
        .acc-header-bg {
            background: #ffffff;
            margin: 0 -1.5rem 0 -1.5rem;
            padding: 14px 32px;
            display: flex;
            align-items: center;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Use columns for header layout
    hcol1, hcol2, hcol3 = st.columns([2, 5, 2])
    
    with hcol1:
        st.markdown("""
            <div style="background:#ffffff; padding:12px 16px; border-radius:0; margin:-1rem -1rem -1rem -1rem;">
                <div style="background:#CC0000; color:white; padding:8px 18px; border-radius:6px; font-family:Inter,sans-serif; font-size:15px; font-weight:700; display:inline-block; margin-bottom:4px;">CDE Catalyst</div>
                <div style="color:#6b7280; font-size:11px; font-family:Inter,sans-serif;">AI Powered CDE governance & risk assessment</div>
            </div>
        """, unsafe_allow_html=True)
    
    with hcol2:
        # Stepper navigation
        step_labels = ["CDE Onboard", "CDE Register", "Action Plan", "Dashboard"]
        current_tab = st.session_state.get('selected_tab', 'CDE Onboard')
        active_idx = step_labels.index(current_tab) if current_tab in step_labels else 0
        
        stepper_parts = []
        for i, label in enumerate(step_labels):
            is_active = (i == active_idx)
            is_completed = (i < active_idx)
            
            lbl_color = "#CC0000" if (is_active or is_completed) else "#6b7280"
            lbl_weight = "600" if (is_active or is_completed) else "400"
            circ_bg = "#CC0000" if (is_active or is_completed) else "transparent"
            circ_border = "#CC0000" if (is_active or is_completed) else "#6b7280"
            circ_color = "white" if (is_active or is_completed) else "#6b7280"
            symbol = "&#10003;" if (is_active or is_completed) else str(i + 1)
            
            stepper_parts.append(f'''<div style="display:flex;flex-direction:column;align-items:center;gap:4px;">
                <span style="font-size:11px;font-weight:{lbl_weight};color:{lbl_color};white-space:nowrap;">{label}</span>
                <div style="width:22px;height:22px;border-radius:50%;background:{circ_bg};border:2px solid {circ_border};color:{circ_color};display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:600;">{symbol}</div>
            </div>''')
            
            if i < len(step_labels) - 1:
                conn_color = "#CC0000" if i < active_idx else "#d1d5db"
                stepper_parts.append(f'<div style="width:50px;height:2px;background:{conn_color};align-self:flex-end;margin-bottom:10px;"></div>')
        
        stepper_html = "".join(stepper_parts)
        st.markdown(f'''
            <div style="background:#ffffff; padding:12px 8px; margin:-1rem -1rem -1rem -1rem; display:flex; align-items:center; justify-content:center; gap:4px;">
                {stepper_html}
            </div>
        ''', unsafe_allow_html=True)
    
    with hcol3:
        # Logo
        logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "ilink_logo.png")
        if os.path.exists(logo_path):
            try:
                with open(logo_path, "rb") as f:
                    logo_b64 = base64.b64encode(f.read()).decode()
                st.markdown(f'''
                    <div style="background:#ffffff; padding:12px 16px; margin:-1rem -1rem -1rem -1rem; display:flex; justify-content:flex-end; align-items:center;">
                        <img src="data:image/png;base64,{logo_b64}" style="height:30px;width:auto;" />
                    </div>
                ''', unsafe_allow_html=True)
            except:
                st.markdown('<div style="background:#ffffff; padding:12px 16px; margin:-1rem -1rem -1rem -1rem; text-align:right; color:#1a1a1a; font-weight:700;">iLink <span style="color:#CC0000;">Digital</span></div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="background:#ffffff; padding:12px 16px; margin:-1rem -1rem -1rem -1rem; text-align:right; color:#1a1a1a; font-weight:700;">iLink <span style="color:#CC0000;">Digital</span></div>', unsafe_allow_html=True)
    
    # Red accent bar
    st.markdown('<div style="background:#CC0000; height:5px; margin:0 -1.5rem 16px -1.5rem;"></div>', unsafe_allow_html=True)


def render_sidebar():
    """Render sidebar with iLink logo, badge, stepper navigation"""
    import base64
    import os
    
    with st.sidebar:
        # iLink logo at top
        logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "ilink_logo.png")
        if os.path.exists(logo_path):
            try:
                with open(logo_path, "rb") as f:
                    logo_b64 = base64.b64encode(f.read()).decode()
                st.markdown(f'''
                    <div style="text-align:center; padding:16px 8px 4px 8px;">
                        <img src="data:image/png;base64,{logo_b64}" style="height:32px;width:auto;" />
                    </div>
                ''', unsafe_allow_html=True)
            except:
                st.markdown('<div style="text-align:center; padding:16px 8px 4px 8px; color:#1a1a1a; font-weight:700; font-family:Inter,sans-serif; font-size:18px;">iLink <span style="color:#CC0000;">Digital</span></div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="text-align:center; padding:16px 8px 4px 8px; color:#1a1a1a; font-weight:700; font-family:Inter,sans-serif; font-size:18px;">iLink <span style="color:#CC0000;">Digital</span></div>', unsafe_allow_html=True)
        
        # App badge
        st.markdown("""
            <div style="text-align:center; padding:4px 8px 12px 8px;">
                <div style="background:#CC0000; color:white; padding:10px 20px; border-radius:6px; font-family:Inter,sans-serif; font-size:16px; font-weight:700; display:inline-block;">CDE Catalyst</div>
                <div style="color:#4b5563; font-size:11px; font-family:Inter,sans-serif; margin-top:6px; font-weight:500;">AI Powered CDE governance & risk assessment</div>
            </div>
        """, unsafe_allow_html=True)
        
        # Spacing before nav
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Stepper navigation with circles
        nav_items = ["CDE Onboard", "CDE Register", "Action Plan", "Dashboard"]
        current_tab = st.session_state.get('selected_tab', 'CDE Onboard')
        
        for label in nav_items:
            is_active = (label == current_tab)
            
            if is_active:
                circ = '<div style="width:22px;height:22px;min-width:22px;border-radius:50%;background:#CC0000;border:2px solid #CC0000;color:white;display:inline-flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;">✓</div>'
                lbl = f'<span style="font-size:13px;font-weight:600;color:#1a1a1a;font-family:Inter,sans-serif;">{label}</span>'
            else:
                circ = '<div style="width:22px;height:22px;min-width:22px;border-radius:50%;background:transparent;border:2px solid #d1d5db;display:inline-flex;align-items:center;justify-content:center;"></div>'
                lbl = f'<span style="font-size:13px;font-weight:400;color:#1a1a1a;font-family:Inter,sans-serif;">{label}</span>'
            
            st.markdown(f'<div style="display:flex;align-items:center;gap:10px;padding:4px 0 4px 20%;justify-content:flex-start;">{circ}{lbl}</div>', unsafe_allow_html=True)
            
            # Clickable button (hidden visually)
            if st.button(label, key=f"nav_{label}", use_container_width=True):
                st.session_state.selected_tab = label
                st.rerun()


# ============================================
# DASHBOARD TAB
# ============================================
def render_dashboard():
    st.markdown("## Dashboard")
    
    # Calculate statistics
    cdes_with_scores = []
    for cde in st.session_state.cdes:
        score = calculate_weighted_score(cde)
        tier = get_risk_tier(score)
        cdes_with_scores.append({**cde, 'weightedScore': score, 'riskTier': tier})
    
    # Count by tier
    tier_counts = {'Critical': 0, 'High': 0, 'Medium': 0, 'Low': 0}
    domain_counts = {}
    total_score = 0
    
    for cde in cdes_with_scores:
        tier_counts[cde['riskTier']] += 1
        domain_counts[cde['domain']] = domain_counts.get(cde['domain'], 0) + 1
        total_score += cde['weightedScore']
    
    avg_score = int(total_score / len(cdes_with_scores)) if cdes_with_scores else 0
    
    # Metric Cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card" style="border-top: 3px solid #2563eb;">
            <p class="metric-label">Total CDEs</p>
            <p class="metric-value" style="color: #2563eb;">{len(st.session_state.cdes)}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card" style="border-top: 3px solid #dc2626;">
            <p class="metric-label">Critical</p>
            <p class="metric-value" style="color: #dc2626;">{tier_counts['Critical']}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-card" style="border-top: 3px solid #ea580c;">
            <p class="metric-label">High</p>
            <p class="metric-value" style="color: #ea580c;">{tier_counts['High']}</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Charts
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.markdown("#### Risk Distribution")
        
        pie_data = [{'Tier': tier, 'Count': count} for tier, count in tier_counts.items() if count > 0]
        if pie_data:
            fig_pie = px.pie(
                pie_data, 
                values='Count', 
                names='Tier',
                color='Tier',
                color_discrete_map={
                    'Critical': '#7f1d1d', # Dark Red
                    'High': '#ea580c', # Orange
                    'Medium': '#d97706', # Yellow/Gold
                    'Low': '#1c1917' # Black
                }
            )
            fig_pie.update_layout(height=300, margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig_pie)
    
    with chart_col2:
        st.markdown("#### CDEs by Domain")
        
        domain_data = [{'Domain': domain, 'Count': count} for domain, count in domain_counts.items()]
        domain_data = [{'Domain': domain, 'Count': count} for domain, count in domain_counts.items()]
        if domain_data:
            fig_bar = px.bar(
                domain_data, 
                x='Domain', 
                y='Count',
                color_discrete_sequence=['#6b7280'] # Grey color
            )
            fig_bar.update_layout(height=300, margin=dict(t=20, b=20, l=20, r=20), showlegend=False)
            st.plotly_chart(fig_bar)
    
    # Critical & High Risk CDEs Table
    # Critical & High Risk CDEs Table
    st.markdown("#### Critical & High Risk CDEs")
    
    critical_high = [cde for cde in cdes_with_scores if cde['riskTier'] in ['Critical', 'High']]
    
    if critical_high:
        df = pd.DataFrame(critical_high)[['id', 'name', 'domain', 'weightedScore', 'riskTier']]
        df.columns = ['ID', 'Name', 'Domain', 'Score', 'Risk Tier']
        
        # Format score for display
        df['Score'] = df['Score'].map(lambda x: f"{x:.2f}")
        
        st.table(df)
    else:
        st.info("No Critical or High risk CDEs found.")

# Add this helper function to show troubleshooting tips

# ============================================
# CDE REGISTER TAB
# ============================================
def render_cde_register():
    st.markdown("### CDE Onboard")
    
    # Sub-tabs for different actions
    onboard_tab1, onboard_tab2, onboard_tab3, onboard_tab4, onboard_tab5 = st.tabs([
        "Add CDE",
        "Upload Excel",
        "Microsoft Purview",
        "AI Recommend", 
        "Identify CDE"
    ])
    
    with onboard_tab1:
        render_cde_add()
    
    with onboard_tab2:
        render_cde_upload()

    with onboard_tab3:
        render_purview_connector()
  
    with onboard_tab4:
        render_ai_recommend()
 
    with onboard_tab5:
        render_identify_cde()

def render_cde_view():
    """View and manage existing CDEs"""
    
    st.write("Count of CDEs:", len(st.session_state.cdes) if "cdes" in st.session_state else 0)
    
    
    # Count by source
    sources_count = {}
    for cde in st.session_state.cdes:
        source = cde.get('sourceSystem', 'Unknown')
        sources_count[source] = sources_count.get(source, 0) + 1
    
    
    st.markdown("---")
    
    # Export and refresh buttons
    col1, col2, col3 = st.columns([6, 1, 1])
    with col2:
        if st.button("Refresh", use_container_width=True, type="primary"):
            st.rerun()
    with col3:
        if st.button("Export", use_container_width=True, type="primary"):
            st.session_state.cde_active_tab = 1
            st.rerun()

    
    # CDE Table
    col_header, col_filter = st.columns([2, 2])
    with col_header:
        st.markdown("#### CDE Registry")
    with col_filter:
        # Use key to persist state for filters
        filter_col1, filter_col2 = st.columns(2)
        with filter_col1:
            show_ai_only = st.checkbox("Show AI Suggestions", value=False, key="show_ai_only")
        with filter_col2:
            show_manual_only = st.checkbox("Show Manual Identified", value=False, key="show_manual_only")
    
    # Calculate scores for display and collect filtered CDEs for management
    display_data = []
    filtered_manage_options = []
    
    for cde in st.session_state.cdes:
        # 🔹 FILTER LOGIC
        if show_ai_only and not cde.get('ai_suggested', False):
            continue
            
        if show_manual_only and not cde.get('manual_qualified', False):
            continue
        
        # Add to management dropdown
        filtered_manage_options.append(f"{cde['id']} - {cde['name']}")
        
        try:
            score = calculate_weighted_score(cde)
            tier = get_risk_tier(score)
            display_data.append({
                'ID': cde.get('id', 'N/A'),
                'Name': cde.get('name', 'N/A'),
                'Domain': cde.get('domain', 'N/A'),
                'Source': cde.get('sourceSystem', 'N/A'),
                'BI': cde.get('businessImpact', 0),
                'RC': cde.get('regulatoryCompliance', 0),
                'DQ': cde.get('dataQualityRisk', 0),
                'SR': cde.get('securityRisk', 0),
                'SC': cde.get('systemComplexity', 0),
                'RD': cde.get('recoveryDifficulty', 0),
                'Score': f"{score:.2f}",
                'Risk': tier
            })
        except Exception as e:
            st.error(f"Error processing CDE {cde.get('id', 'Unknown')}: {str(e)}")
    
    if display_data:
        df = pd.DataFrame(display_data)
        
        # Count CDEs by source
        initial_cdes = len([c for c in st.session_state.cdes if c.get('sourceSystem') not in ['Purview', 'Microsoft Fabric', 'Excel', 'AI Suggestion']])
        purview_cdes = len([c for c in st.session_state.cdes if c.get('sourceSystem') == 'Purview'])
        fabric_cdes = len([c for c in st.session_state.cdes if c.get('sourceSystem') == 'Microsoft Fabric'])
        excel_cdes = len([c for c in st.session_state.cdes if c.get('sourceSystem') == 'Excel'])
        ai_cdes = len([c for c in st.session_state.cdes if c.get('sourceSystem') == 'AI Suggestion'])
        
        st.write(f"**Displaying {len(df)} CDEs** | Initial: {initial_cdes} | Purview: {purview_cdes} | Fabric: {fabric_cdes} | Excel: {excel_cdes} | AI: {ai_cdes}")
        
        # Use st.table for a clean, static look that matches the reference image
        st.table(df)
        
        # Edit/Delete section
        st.markdown("---")
        st.markdown("##### Manage CDEs")
        
        # Reduce width using columns
        cde_col1, cde_col2 = st.columns([1, 1])
        with cde_col1:
            selected_cde = st.selectbox("Select CDE to manage:", ["Select"] + filtered_manage_options, key="manage_cde_select")
        
        if selected_cde and selected_cde != "Select":
            cde_id = selected_cde.split(" - ")[0]
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Edit CDE", type="primary", key="edit_cde_btn"):
                    st.session_state.editing_cde_id = cde_id
                    st.session_state.show_cde_form = True
                    st.rerun()
            with col2:
                if st.button("Delete CDE", type="primary", key="delete_cde_btn"):
                    st.session_state.cdes = [c for c in st.session_state.cdes if c['id'] != cde_id]
                    st.success(f"Deleted {cde_id}")
                    st.rerun()
    else:
        st.info("No CDEs found. Add CDEs manually or upload from Excel/Purview.")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Show edit form if editing
    if st.session_state.get('show_cde_form', False):
        render_cde_form()

def render_cde_add():
    """Add a new CDE manually"""
    st.markdown("#### Add New CDE")
    st.markdown("Fill in the details below to add a new Critical Data Element.")
    
    # Callback to sync form data
    def sync_add_form():
        st.session_state.onboard_form_data['add_name'] = st.session_state.aname
        st.session_state.onboard_form_data['add_domain'] = st.session_state.adom
        st.session_state.onboard_form_data['add_source'] = st.session_state.asrc
        st.session_state.onboard_form_data['add_steward'] = st.session_state.aste
        st.session_state.onboard_form_data['add_owner'] = st.session_state.aown
        st.session_state.onboard_form_data['add_def'] = st.session_state.adef

    with st.container(border=True):
        col1, col2 = st.columns(2)
        
        formData = st.session_state.onboard_form_data
        
        with col1:
            name = st.text_input("CDE Name *", value=formData['add_name'], key="aname", on_change=sync_add_form)
            domain = st.selectbox("Domain", DOMAINS, index=DOMAINS.index(formData['add_domain']) if formData['add_domain'] in DOMAINS else 0, key="adom", on_change=sync_add_form)
            source_system = st.text_input("Source System", value=formData['add_source'], key="asrc", on_change=sync_add_form)
        
        with col2:
            steward = st.text_input("Data Steward", value=formData['add_steward'], key="aste", on_change=sync_add_form)
            owner = st.text_input("Data Owner", value=formData['add_owner'], key="aown", on_change=sync_add_form)
            definition = st.text_area("Definition/Description", value=formData['add_def'], height=100, key="adef", on_change=sync_add_form)
        
        st.markdown("##### Impact Assessment (1-5)")
        
        score_col1, score_col2, score_col3 = st.columns(3)
        
        with score_col1:
            business_impact = st.slider("Business Impact (25%)", 1, 5, 3, key="add_bi")
            regulatory = st.slider("Regulatory (20%)", 1, 5, 3, key="add_rc")
        
        with score_col2:
            data_quality = st.slider("Data Quality (20%)", 1, 5, 3, key="add_dq")
            security = st.slider("Security (15%)", 1, 5, 3, key="add_sr")
        
        with score_col3:
            complexity = st.slider("Complexity (10%)", 1, 5, 3, key="add_sc")
            recovery = st.slider("Recovery (10%)", 1, 5, 3, key="add_rd")
        
        # Calculate preview score
        preview_score = round(
            business_impact * 0.25 +
            regulatory * 0.20 +
            data_quality * 0.20 +
            security * 0.15 +
            complexity * 0.10 +
            recovery * 0.10, 2
        )
        preview_tier = get_risk_tier(preview_score)
        
        st.markdown(f"""
        <div style="padding: 16px; border-radius: 8px; background-color: {get_risk_bg(preview_tier)}; display: flex; justify-content: space-between; align-items: center; margin: 16px 0;">
            <div style="font-family: 'Inter', sans-serif;">
                <span style="color: #6b7280; font-size: 14px;">Assessment Preview:</span><br>
                <span style="font-size: 28px; font-weight: 700;">{preview_score}</span>
            </div>
            {render_risk_badge(preview_tier)}
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("Add CDE", type="primary", use_container_width=False):
            if name:
                new_cde = {
                    'id': f"CDE-{str(len(st.session_state.cdes) + 1).zfill(3)}",
                    'name': name,
                    'domain': domain,
                    'sourceSystem': source_system,
                    'steward': steward,
                    'owner': owner,
                    'definition': definition,
                    'businessImpact': business_impact,
                    'regulatoryCompliance': regulatory,
                    'dataQualityRisk': data_quality,
                    'securityRisk': security,
                    'systemComplexity': complexity,
                    'recoveryDifficulty': recovery,
                    'dataType': '',
                    'downstreamSystems': '',
                    'regulatory': '',
                    'status': 'Active',
                    'assessmentDate': datetime.now().strftime('%Y-%m-%d'),
                    'notes': ''
                }
                st.session_state.cdes.append(new_cde)
                # Clear persistent form after successful add
                st.session_state.onboard_form_data = {
                    'add_name': '', 'add_domain': DOMAINS[0], 'add_source': '', 'add_steward': '', 
                    'add_owner': '', 'add_def': '', 'eval_selected_cde': None
                }
                st.success(f"✅ Added CDE: {name} ({new_cde['id']})")
                st.rerun()
            else:
                st.error("Please enter a CDE name")
def get_unique_values(df, col):
    if not col or col not in df.columns:
        return [""]
    return [""] + (
        df[col]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )


def render_cde_upload():
    import pandas as pd
    import streamlit as st
    from datetime import datetime

    st.markdown("#### Upload CDEs from Excel")
    st.markdown("Upload an Excel file containing CDE data to bulk import.")
    
    
    # Download template section
    st.markdown("##### Step 1: Download Template")
    st.markdown("Download the Excel template, fill in your CDE data, then upload it below.")
    
    template_data = create_cde_template()
    st.download_button(
        "Download Excel Template",
        template_data,
        file_name="CDE_Upload_Template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="content",
        type="primary"
    )
    
    # Upload section
    st.markdown("##### Step 2: Upload Your File")
    # --------------------------------------------------
    # Upload
    # --------------------------------------------------
    uploaded_file = st.file_uploader(
        "Choose Excel / CSV file",
        type=["xlsx", "xls", "csv"]
    )

    if not uploaded_file:
        return

    if uploaded_file.name.endswith('.csv'):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)
        
    st.success(f"✅ Loaded: {uploaded_file.name}")

    with st.expander("Preview Data", expanded=True):
        st.table(df.head(10))



    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------
    def find_col(possible):
        for c in df.columns:
            key = c.lower().replace(" ", "").replace("_", "")
            for p in possible:
                if p in key:
                    return c
        return None

    def values(col):
        if not col:
            return []
        return sorted(df[col].dropna().astype(str).unique().tolist())

    def score_values():
        return ["", 1, 2, 3, 4, 5]

    # --------------------------------------------------
    # Auto column detection
    # --------------------------------------------------
    name_col = find_col(["name", "cde", "element"])
    domain_col = find_col(["domain"])
    source_col = find_col(["source"])
    steward_col = find_col(["steward"])
    owner_col = find_col(["owner"])
    definition_col = find_col(["definition", "description"])
    bi_col = find_col(["businessimpact", "bi"])
    rc_col = find_col(["regulatory", "rc"])
    dq_col = find_col(["dataquality", "dq"])
    sr_col = find_col(["security", "sr"])
    sc_col = find_col(["complexity", "sc"])
    rd_col = find_col(["recovery", "rd"])

    # --------------------------------------------------
    # UI — VALUE DROPDOWNS
    # --------------------------------------------------
    st.markdown("##### CDE Values")

    c1, c2, c3 = st.columns(3)

    with c1:
        sel_name = st.selectbox(
            "CDE Name",
            values(name_col),
            index=None,
            placeholder="(Leave empty to import all)"
        )
        sel_domain = st.selectbox("Domain", values(domain_col), index=None)

    with c2:
        # Column 2 - VISIBLE TEXT INPUTS
        sel_steward = st.text_input(
            "Data Steward",
            value="",
            placeholder="e.g., John Smith",
            help="Enter Data Steward name (optional)"
        )
        sel_owner = st.text_input(
            "Data Owner",
            value="",
            placeholder="e.g., Jane Doe",
            help="Enter Data Owner name (optional)"
        )

    with c3:
        sel_source = st.selectbox("Source System", values(source_col), index=None)
        sel_definition = st.selectbox("Definition", values(definition_col), index=None)

    r1, r2, r3 = st.columns(3)
    with r1:
        bi = st.selectbox("Business Impact (1–5)", score_values(), index=0)
    with r2:
        rc = st.selectbox("Regulatory Compliance (1–5)", score_values(), index=0)
    with r3:
        dq = st.selectbox("Data Quality Risk (1–5)", score_values(), index=0)

    r4, r5, r6 = st.columns(3)
    with r4:
        sr = st.selectbox("Security Risk (1–5)", score_values(), index=0)
    with r5:
        sc = st.selectbox("System Complexity (1–5)", score_values(), index=0)
    with r6:
        rd = st.selectbox("Recovery Difficulty (1–5)", score_values(), index=0)

    # --------------------------------------------------
    # IMPORT LOGIC (CRITICAL PART)
    # --------------------------------------------------
    if st.button("Import CDE", type="primary"):

        # 🔹 IF NOTHING SELECTED → IMPORT ALL
        if not sel_name:
            rows_to_import = df.copy()
        else:
            rows_to_import = df[df[name_col].astype(str) == sel_name]

        def score(val, excel_val):
            if val in [1, 2, 3, 4, 5]:
                return int(val)
            if pd.notna(excel_val):
                try:
                    return int(excel_val)
                except:
                    return 3
            return 3

        imported = 0
        skipped = 0

        for _, row in rows_to_import.iterrows():
            name = str(row[name_col]).strip()
            if not name:
                continue

            # Check for duplicates
            if any(c['name'] == name for c in st.session_state.cdes):
                skipped += 1
                continue

            new_cde = {
                "id": f"CDE-{len(st.session_state.cdes)+1:03}",
                "name": name,
                "domain": sel_domain or str(row.get(domain_col, "Reference")),
                "sourceSystem": sel_source or str(row.get(source_col, "")),
                "steward": sel_steward or str(row.get(steward_col, "")),
                "owner": sel_owner or str(row.get(owner_col, "")),
                "definition": sel_definition or str(row.get(definition_col, "")),
                "businessImpact": score(bi, row.get(bi_col)),
                "regulatoryCompliance": score(rc, row.get(rc_col)),
                "dataQualityRisk": score(dq, row.get(dq_col)),
                "securityRisk": score(sr, row.get(sr_col)),
                "systemComplexity": score(sc, row.get(sc_col)),
                "recoveryDifficulty": score(rd, row.get(rd_col)),
                "status": "Active",
                "assessmentDate": datetime.now().strftime("%Y-%m-%d"),
                "notes": "Imported from Excel",
            }

            st.session_state.cdes.append(new_cde)
            imported += 1

        if imported > 0:
            st.success(f"✅ Imported {imported} new CDE(s).")
        
        if skipped > 0:
            st.info(f"ℹ️ Skipped {skipped} duplicate CDE(s) that already exist in the register.")
            
        if imported > 0:
            st.rerun()
    
    # Instructions
    st.markdown("##### Instructions")
    st.markdown("""
    1. **Download the template** using the button above
    2. **Fill in your CDE data** in Excel:
       - **Name**: Required - the name of the data element
       - **Domain**: Category (Customer, Account, Transaction, etc.)
       - **Risk Scores**: Values from 1-5 (or leave blank for default of 3)
    3. **Upload the file** and map your columns
    4. **Click Import** to add CDEs to the register
    
    **Supported formats:** `.xlsx`, `.xls`, `.csv`
    """)

def normalize_cde(cde, source="Purview", new_id=None):
    """Normalize CDE from any source to standard format"""
    # Use provided ID or get from cde
    cde_id = new_id if new_id else cde.get("id", f"CDE-TEMP")
    
    return {
        "id": cde_id,
        "name": cde.get("name", ""),
        "domain": cde.get("domain") if cde.get("domain") in DOMAINS else "Reference",
        "definition": cde.get("description", cde.get("definition", "")),
        "dataType": cde.get("dataType", ""),
        "sourceSystem": cde.get("sourceSystem", source),
        "steward": cde.get("steward", ""),
        "owner": cde.get("owner", ""),
        "downstreamSystems": cde.get("downstreamSystems", ""),
        "regulatory": cde.get("regulatory", ""),
        "businessImpact": int(cde.get("businessImpact", 3)),
        "regulatoryCompliance": int(cde.get("regulatoryCompliance", 3)),
        "dataQualityRisk": int(cde.get("dataQualityRisk", 3)),
        "securityRisk": int(cde.get("securityRisk", 3)),
        "systemComplexity": int(cde.get("systemComplexity", 3)),
        "recoveryDifficulty": int(cde.get("recoveryDifficulty", 3)),
        "status": cde.get("status", "Active"),
        "assessmentDate": cde.get("assessmentDate", datetime.now().strftime("%Y-%m-%d")),
        "notes": cde.get("notes", f"Imported from {source}")
    }

def render_purview_connector():
    """Connect to Microsoft Purview to import CDEs"""
    # Callback for persistence
    def sync_purview():
        st.session_state.connector_creds['purview_account_name'] = st.session_state.p_acc
        st.session_state.connector_creds['purview_tenant_id'] = st.session_state.p_ten
        st.session_state.connector_creds['purview_client_id'] = st.session_state.p_cli
        st.session_state.connector_creds['purview_client_secret'] = st.session_state.p_sec

    # Header with Logo
    col_logo, col_text = st.columns([1, 15])
    with col_logo:
        try:
            # Load and encode logo to apply custom margin (st.image doesn't support margin)
            import base64
            import os
            logo_path = os.path.join(os.path.dirname(__file__), "assets", "purview_Logo.png")
            with open(logo_path, "rb") as f:
                logo_data = base64.b64encode(f.read()).decode()
            st.markdown(f'<img src="data:image/png;base64,{logo_data}" style="width: 40px; margin-top: 10px;">', unsafe_allow_html=True)
        except:
            st.markdown("")
    with col_text:
        st.markdown("#### Microsoft Purview Connector")
    st.markdown("Connect to Microsoft Purview to automatically import Critical Data Elements from your data catalog.")
    
    # Connection section
    with st.container(border=True):
        st.markdown("##### Connection Settings")
        
        col1, col2 = st.columns(2)
        with col1:
            purview_account = st.text_input("Purview Account Name", value=st.session_state.connector_creds['purview_account_name'], key="p_acc", on_change=sync_purview)
            tenant_id = st.text_input("Tenant ID", type="password", value=st.session_state.connector_creds['purview_tenant_id'], key="p_ten", on_change=sync_purview)
        with col2:
            client_id = st.text_input("Client ID", type="password", value=st.session_state.connector_creds['purview_client_id'], key="p_cli", on_change=sync_purview)
            client_secret = st.text_input("Client Secret", type="password", value=st.session_state.connector_creds['purview_client_secret'], key="p_sec", on_change=sync_purview)
    
       
    # Fetch and Export CDEs button
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Fetch CDEs", use_container_width=True, type="primary"):
            if not all([purview_account, tenant_id, client_id, client_secret]):
                st.error("Please fill in all required fields")
            else:
                with st.spinner("🔄 Fetching CDEs from Purview..."):
                    try:
                        connector = PurviewConnector(
                            purview_account,
                            tenant_id,
                            client_id,
                            client_secret
                        )
                        
                        # Authenticate
                        success, auth_msg = connector.authenticate()
                        if not success:
                            st.error(f"❌ Authentication failed: {auth_msg}")
                            st.stop()
                        
                        st.success("✅ Authentication successful!")
                        
                        # Fetch CDEs
                        cdes = connector.fetch_cdes(debug=True)
                        
                        if cdes:
                            st.success(f"✅ Found {len(cdes)} CDEs!")
                            
                            # Store in session state for export
                            st.session_state.purview_cdes = cdes
                            
                            
                        else:
                            st.warning("⚠️ No CDEs found in Purview")
                            
                    except Exception as e:
                        st.error(f"❌ Error fetching CDEs: {str(e)}")
                        import traceback
                        with st.expander("🐛 Debug Information"):
                            st.code(traceback.format_exc())
    
    # --------------------------------------------------
    # PERSISTENT DISPLAY OF FETCHED CDES
    # --------------------------------------------------

    if 'purview_cdes' in st.session_state and st.session_state.purview_cdes:
        st.markdown("---")
        st.markdown("### Purview CDEs")
        
        # Preview
        with st.expander("Values Found", expanded=True):
            preview_df = pd.DataFrame([{
                'Name': c.get('name', 'N/A'),
                'Domain': c.get('domain', 'Reference'),
                'Description': c.get('description', '')[:50] + '...' if c.get('description', '') else '',
                'Steward': c.get('steward', ''),
                'Status': c.get('status', 'Active')
            } for c in st.session_state.purview_cdes])
            st.table(preview_df)
        
        # Import Button
        if st.button("Import CDEs to Register", type="primary"):
             count = 0
             skipped = 0
             for item in st.session_state.purview_cdes:
                 # Check for duplicates based on name
                 if any(existing['name'] == item['name'] for existing in st.session_state.cdes):
                     skipped += 1
                     continue
                 
                 # Normalize and map Purview item to CDE
                 new_cde = normalize_cde(item, source="Purview", new_id=f"CDE-{len(st.session_state.cdes)+1:03}")
                 st.session_state.cdes.append(new_cde)
                 count += 1
             
             if count > 0:
                 st.session_state.purview_import_successful = True
                 st.session_state.purview_imported_count = count
                 st.session_state.switch_to_view_tab = True
                 st.success(f"✅ Imported {count} new CDEs from Purview!")
             
             if skipped > 0:
                 st.info(f"ℹ️ Skipped {skipped} duplicate items that already exist in the register.")
                 
             if count > 0:
                 st.rerun()
             elif count == 0 and skipped == 0:
                 st.warning("All items already exist in the registry.")
        
        # Show persistent import success prompt below the button
        if st.session_state.get('purview_import_successful'):
            st.success("✅ Imported")



    
    with col2:
        if st.button("Export to Excel", type="secondary", use_container_width=True, disabled='purview_cdes' not in st.session_state):
            if 'purview_cdes' in st.session_state:
                try:
                    # Prepare data for Excel
                    export_data = []
                    for cde in st.session_state.purview_cdes:
                        export_data.append({
                            'Name': cde.get('name', 'N/A'),
                            'Domain': cde.get('domain', 'Reference'),
                            'Definition': cde.get('description', ''),
                            'Source System': cde.get('sourceSystem', 'Purview'),
                            'Data Steward': cde.get('steward', ''),
                            'Data Owner': cde.get('owner', ''),
                            'Business Impact (1-5)': 3,
                            'Regulatory Compliance (1-5)': 3,
                            'Data Quality Risk (1-5)': 3,
                            'Security Risk (1-5)': 3,
                            'System Complexity (1-5)': 3,
                            'Recovery Difficulty (1-5)': 3
                        })
                    
                    df = pd.DataFrame(export_data)
                    
                    # Create Excel file
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df.to_excel(writer, sheet_name='Purview CDEs', index=False)
                    
                    excel_data = output.getvalue()
                    
                    # Download button
                    st.download_button(
                        "Download Excel",
                        excel_data,
                        file_name=f"Purview_CDEs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                    st.success("✅ Excel file ready for download!")
                    
                except Exception as e:
                    st.error(f"❌ Error creating Excel: {str(e)}")
    
    # Instructions
    st.markdown("##### Instructions")
    st.markdown("""
    **Workflow:**
    1. **Enter credentials** and click "Test Connection"
    2. **Fetch CDEs** from Purview
    3. **Import CDEs** - directly imported.
    4. **Export to Excel** - optional backup of fetched items
    5. **Edit CDEs** as needed using the Edit form to add risk scores
    
    **Setup Requirements:**
    - Azure AD App Registration with Purview permissions
    - Service Principal with "Data Reader" role in Purview
    - Tenant ID, Client ID, Client Secret
    """)

def render_fabric_connector():
    """Connect to Microsoft Fabric to import CDEs"""
    # Callback for persistence
    def sync_fabric():
        st.session_state.connector_creds['fabric_tenant_id'] = st.session_state.f_ten
        st.session_state.connector_creds['fabric_client_id'] = st.session_state.f_cli
        st.session_state.connector_creds['fabric_client_secret'] = st.session_state.f_sec

    # Header with Logo
    col_logo, col_text = st.columns([1, 15])
    with col_logo:
        try:
            st.image("assets/fabric_Logo.png", width=40)
        except:
            st.markdown("") 
    with col_text:
        st.markdown("#### Microsoft Fabric Connector")

    st.markdown("Connect to Microsoft Fabric to import workspaces and items as Critical Data Elements.")
    
    # Connection section
    with st.container(border=True):
        st.markdown("##### Connection Settings")
        
        col1, col2 = st.columns(2)
        with col1:
            fabric_tenant_id = st.text_input("Fabric Tenant ID", type="password", help="Azure AD Tenant ID", value=st.session_state.connector_creds['fabric_tenant_id'], key="f_ten", on_change=sync_fabric)
        with col2:
            fabric_client_id = st.text_input("Client ID", type="password", help="Service Principal Client ID", value=st.session_state.connector_creds['fabric_client_id'], key="f_cli", on_change=sync_fabric)
            fabric_client_secret = st.text_input("Client Secret", type="password", help="Service Principal Secret", value=st.session_state.connector_creds['fabric_client_secret'], key="f_sec", on_change=sync_fabric)
       
    # Fetch and Export CDEs button
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Fetch Items", use_container_width=True, type="primary"):
            if not all([fabric_tenant_id, fabric_client_id, fabric_client_secret]):
                st.error("Please fill in all required fields")
            else:
                with st.spinner("🔄 Fetching Items from Fabric..."):
                    try:
                        connector = FabricConnector(
                            fabric_tenant_id,
                            fabric_client_id,
                            fabric_client_secret
                        )
                        
                        # Authenticate
                        success, auth_msg = connector.authenticate()
                        if not success:
                            st.error(f"❌ Authentication failed: {auth_msg}")
                            st.stop()
                        
                        st.success("✅ Authentication successful!")
                        
                        # Fetch CDEs
                        cdes = connector.fetch_cdes(debug=True)
                        
                        if cdes:
                            st.success(f"✅ Found {len(cdes)} Fabric Items!")
                            
                            # Store in session state for export
                            st.session_state.fabric_cdes = cdes
                            
                        else:
                            st.warning("⚠️ No items found in Fabric")
                            
                    except Exception as e:
                        st.error(f"❌ Error fetching Fabric items: {str(e)}")
                        import traceback
                        with st.expander("🐛 Debug Information"):
                            st.code(traceback.format_exc())
    
    # --------------------------------------------------
    # PERSISTENT DISPLAY OF FETCHED CDES
    # --------------------------------------------------

    if 'fabric_cdes' in st.session_state and st.session_state.fabric_cdes:
        st.markdown("---")
        st.markdown("### Fabric Items Found")
        
        # Preview
        with st.expander("Values Found", expanded=True):
            preview_df = pd.DataFrame([{
                'Name': c.get('name', 'N/A'),
                'Type': c.get('dataType', 'Unknown'),
                'Domain': c.get('domain', 'Reference'),
                'Description': c.get('description', '')[:50] + '...' if c.get('description', '') else '',
                'Workspace': c.get('notes', '').replace('Imported from Fabric Workspace: ', '')
            } for c in st.session_state.fabric_cdes])
            st.table(preview_df)
        
        # Import Button
        if st.button("Import All Fabric Items to CDE Register", type="primary"):
             count = 0
             skipped = 0
             for item in st.session_state.fabric_cdes:
                 # Check for duplicates based on name
                 if any(existing['name'] == item['name'] for existing in st.session_state.cdes):
                     skipped += 1
                     continue
                     
                 new_cde = item.copy()
                 new_cde['id'] = f"CDE-{len(st.session_state.cdes)+1:03}"
                 st.session_state.cdes.append(new_cde)
                 count += 1
             
             if count > 0:
                 st.session_state.fabric_import_successful = True
                 st.session_state.fabric_imported_count = count
                 st.session_state.switch_to_view_tab = True
                 st.success(f"✅ Imported {count} new CDEs from Fabric!")
             
             if skipped > 0:
                 st.info(f"ℹ️ Skipped {skipped} duplicate items that already exist in the register.")
                 
             if count > 0:
                 st.rerun()
             elif count == 0 and skipped == 0:
                 st.warning("All items already exist in the registry.")

        # Show persistent import success prompt below the button
        if st.session_state.get('fabric_import_successful'):
            st.success("✅ Imported")

    
    with col2:
        if st.button("Export to Excel", type="secondary", use_container_width=True, key="fabric_export_btn", disabled='fabric_cdes' not in st.session_state):
            if 'fabric_cdes' in st.session_state:
                try:
                    # Prepare data for Excel
                    export_data = []
                    for cde in st.session_state.fabric_cdes:
                        export_data.append({
                            'Name': cde.get('name', 'N/A'),
                            'Type': cde.get('dataType', ''),
                            'Domain': cde.get('domain', 'Reference'),
                            'Definition': cde.get('description', ''),
                            'Source System': cde.get('sourceSystem', 'Microsoft Fabric'),
                            'Data Steward': cde.get('steward', ''),
                            'Data Owner': cde.get('owner', ''),
                            'Business Impact (1-5)': 3,
                            'Regulatory Compliance (1-5)': 3,
                            'Data Quality Risk (1-5)': 3,
                            'Security Risk (1-5)': 3,
                            'System Complexity (1-5)': 3,
                            'Recovery Difficulty (1-5)': 3
                        })
                    
                    df = pd.DataFrame(export_data)
                    
                    # Create Excel file
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df.to_excel(writer, sheet_name='Fabric Items', index=False)
                    
                    excel_data = output.getvalue()
                    
                    # Download button
                    st.download_button(
                        "Download Excel",
                        excel_data,
                        file_name=f"Fabric_Items_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                    st.success("✅ Excel file ready for download!")
                    
                except Exception as e:
                    st.error(f"❌ Error creating Excel: {str(e)}")
    
    # Instructions
    st.markdown("##### Instructions")
    st.markdown("""
    **Workflow:**
    1. **Enter Service Principal credentials** (Tenant ID, Client ID, Secret).
    2. **Fetch Items** from Fabric Workspaces.
    3. **Import directly** to register or **Export to Excel** for manual review.
    """)

def render_fabric_table_import():
    """Import CDEs by scanning a specific Microsoft Fabric table schema"""
    def sync_fabric_sql():
        st.session_state.connector_creds['fabric_sql_endpoint'] = st.session_state.f_sql
        st.session_state.connector_creds['fabric_table_name'] = st.session_state.f_tab

    # Header with Logo
    col_logo, col_text = st.columns([1, 15])
    with col_logo:
        try:
            st.image("assets/fabric_Logo.png", width=40)
        except:
            st.markdown("") 
    with col_text:
        st.markdown("#### Microsoft Fabric Table Import")

    st.markdown("Connect to a specific Microsoft Fabric table via its SQL Endpoint to identify Critical Data Elements using AI.")
    
    # Connection section
    with st.container(border=True):
        st.markdown("##### SQL Endpoint Connection")
        
        f_sql = st.text_input("SQL Endpoint / Connection String", 
                             help="e.g. xxxxxxxx.datawarehouse.fabric.microsoft.com;Authentication=ActiveDirectoryInteractive", 
                             value=st.session_state.connector_creds['fabric_sql_endpoint'], 
                             key="f_sql", on_change=sync_fabric_sql)
        f_tab = st.text_input("Table Name", placeholder="e.g. Sales_Transactions", value=st.session_state.connector_creds['fabric_table_name'], key="f_tab", on_change=sync_fabric_sql)
        
    # Fetch and Recommend
    if st.button("Fetch Schema & Recommend CDEs", type="primary", use_container_width=True):
        if not all([f_sql, f_tab]):
            st.error("Please enter both the SQL Endpoint and Table Name.")
        else:
            with st.spinner(f"🔄 Fetching schema for '{f_tab}' from Fabric..."):
                try:
                    from backend.fabric_connector import FabricConnector
                    from backend.ai_recommender import AIRecommender
                    
                    # Use stored credentials
                    creds = st.session_state.connector_creds
                    connector = FabricConnector(
                        creds.get('fabric_tenant_id', ''),
                        creds.get('fabric_client_id', ''),
                        creds.get('fabric_client_secret', '')
                    )
                    schema = connector.fetch_table_schema(f_sql, f_tab)
                    
                    if schema:
                        st.success(f"✅ Found {len(schema)} columns in '{f_tab}'.")
                        
                        with st.spinner("Analyzing..."):
                            recommender = AIRecommender()
                            cols_list = [c['name'] for c in schema]
                            recommendations = recommender.recommend_cdes_from_columns(f_tab, cols_list)
                            
                            if recommendations:
                                st.session_state.candidate_queue = recommendations
                                st.success(f"✅ AI suggested {len(recommendations)} potential CDEs!")
                                st.session_state.onboard_sub_tab = "AI Recommend"
                                st.rerun()
                            else:
                                st.warning("⚠️ AI could not identify any CDEs from this schema.")
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")

    st.markdown("##### Instructions")
    st.markdown("""
    1. **Copy the SQL Endpoint** from your Fabric Lakehouse or Warehouse settings.
    2. **Enter the Table Name** you want to scan.
    3. **Click Fetch & Recommend** to start the AI analysis.
    4. Suggested CDEs will appear in the **AI Recommend** tab for your review.
    """)

def get_score_value(row, col, use_default=True):
    """Get a score value from a row, with validation"""
    if not col:
        return 3 if use_default else 1
    
    try:
        val = row.get(col)
        if pd.isna(val):
            return 3 if use_default else 1
        val = int(float(val))
        return max(1, min(5, val))  # Clamp between 1-5
    except:
        return 3 if use_default else 1

def create_cde_template():
    """Create an Excel template for CDE upload"""
    template_data = {
        'Name': ['Customer ID', 'Account Balance', 'Transaction Amount'],
        'Domain': ['Customer', 'Account', 'Transaction'],
        'Definition': ['Unique customer identifier', 'Current account balance', 'Transaction monetary value'],
        'Source System': ['Core Banking', 'Core Banking', 'Payment System'],
        'Data Steward': ['John Smith', 'Jane Doe', 'Bob Wilson'],
        'Data Owner': ['Data Governance', 'Data Governance', 'Data Governance'],
        'Business Impact (1-5)': [4, 5, 5],
        'Regulatory Compliance (1-5)': [3, 5, 4],
        'Data Quality Risk (1-5)': [3, 3, 3],
        'Security Risk (1-5)': [4, 4, 3],
        'System Complexity (1-5)': [3, 4, 4],
        'Recovery Difficulty (1-5)': [2, 3, 3]
    }
    
    df = pd.DataFrame(template_data)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='CDE Data', index=False)
        
        # Add instructions sheet
        instructions = pd.DataFrame({
            'Field': ['Name', 'Domain', 'Definition', 'Source System', 'Data Steward', 'Data Owner', 
                     'Business Impact', 'Regulatory Compliance', 'Data Quality Risk', 
                     'Security Risk', 'System Complexity', 'Recovery Difficulty'],
            'Required': ['Yes', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No'],
            'Description': [
                'Name of the Critical Data Element (Required)',
                'Data domain: Customer, Account, Transaction, Product, Employee, Risk, Financial, Reference',
                'Description of what this data element represents',
                'System where data originates',
                'Person responsible for data stewardship',
                'Business owner of the data',
                'Score 1-5: Impact on business if data is incorrect',
                'Score 1-5: Regulatory requirements for this data',
                'Score 1-5: Risk of data quality issues',
                'Score 1-5: Security and privacy sensitivity',
                'Score 1-5: Complexity of systems using this data',
                'Score 1-5: Difficulty to recover/correct data issues'
            ]
        })
        instructions.to_excel(writer, sheet_name='Instructions', index=False)
    
    return output.getvalue()

def export_cdes_to_excel(cdes_list=None):
    """Export CDEs to Excel file"""
    export_data = []
    
    # Use provided list or default to all
    source_cdes = cdes_list if cdes_list is not None else st.session_state.cdes
    
    for cde in source_cdes:
        score = calculate_weighted_score(cde)
        tier = get_risk_tier(score)
        
        export_data.append({
            'ID': cde['id'],
            'Name': cde['name'],
            'Domain': cde['domain'],
            'Definition': cde.get('definition', ''),
            'Source System': cde.get('sourceSystem', ''),
            'Data Steward': cde.get('steward', ''),
            'Data Owner': cde.get('owner', ''),
            'Business Impact': cde['businessImpact'],
            'Regulatory Compliance': cde['regulatoryCompliance'],
            'Data Quality Risk': cde['dataQualityRisk'],
            'Security Risk': cde['securityRisk'],
            'System Complexity': cde['systemComplexity'],
            'Recovery Difficulty': cde['recoveryDifficulty'],
            'Risk Score': score,
            'Risk Tier': tier,
            'Status': cde.get('status', 'Active'),
            'Assessment Date': cde.get('assessmentDate', '')
        })
    
    df = pd.DataFrame(export_data)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='CDE Register', index=False)
    
    return output.getvalue()

def render_cde_form():
    """Render CDE add/edit form"""
    st.markdown("---")
    st.markdown("### CDE Form")
    
    # Check if editing
    editing_id = st.session_state.get('editing_cde_id', None)
    editing_cde = None
    if editing_id:
        editing_cde = next((c for c in st.session_state.cdes if c['id'] == editing_id), None)
    
    with st.form("cde_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            name = st.text_input("CDE Name *", value=editing_cde['name'] if editing_cde else "")
            
            # ✅ FIX: Safe domain index lookup
            domain_index = 0
            if editing_cde and editing_cde['domain'] in DOMAINS:
                domain_index = DOMAINS.index(editing_cde['domain'])
            
            domain = st.selectbox("Domain", DOMAINS, index=domain_index)
        
        with col2:
            source_system = st.text_input("Source System", value=editing_cde.get('sourceSystem', '') if editing_cde else "")
            steward = st.text_input("Data Steward", value=editing_cde.get('steward', '') if editing_cde else "")
        
        st.markdown("##### Impact Assessment (1-5)")
        
        score_col1, score_col2, score_col3 = st.columns(3)
        
        with score_col1:
            business_impact = st.slider("Business Impact (25%)", 1, 5, editing_cde['businessImpact'] if editing_cde else 3)
            regulatory = st.slider("Regulatory (20%)", 1, 5, editing_cde['regulatoryCompliance'] if editing_cde else 3)
        
        with score_col2:
            data_quality = st.slider("Data Quality (20%)", 1, 5, editing_cde['dataQualityRisk'] if editing_cde else 3)
            security = st.slider("Security (15%)", 1, 5, editing_cde['securityRisk'] if editing_cde else 3)
        
        with score_col3:
            complexity = st.slider("Complexity (10%)", 1, 5, editing_cde['systemComplexity'] if editing_cde else 3)
            recovery = st.slider("Recovery (10%)", 1, 5, editing_cde['recoveryDifficulty'] if editing_cde else 3)
        
        # Calculate preview score
        preview_score = round(
            business_impact * 0.25 +
            regulatory * 0.20 +
            data_quality * 0.20 +
            security * 0.15 +
            complexity * 0.10 +
            recovery * 0.10, 2
        )
        preview_tier = get_risk_tier(preview_score)
        
        st.markdown(f"""
        <div style="padding: 16px; border-radius: 8px; background-color: {get_risk_bg(preview_tier)}; display: flex; justify-content: space-between; align-items: center; margin: 16px 0;">
            <div><span style="color: #6b7280;">Score: </span><span style="font-size: 24px; font-weight: bold;">{preview_score}</span></div>
            {render_risk_badge(preview_tier)}
        </div>
        """, unsafe_allow_html=True)
        
        # ✅ FIX: Add submit button
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            submitted = st.form_submit_button("Save CDE", use_container_width=True, type="primary")
        with col2:
            cancelled = st.form_submit_button("Cancel", use_container_width=True, type="secondary")
        
        if submitted and name:
            new_cde = {
                'id': editing_id if editing_id else f"CDE-{str(len(st.session_state.cdes) + 1).zfill(3)}",
                'name': name,
                'domain': domain,
                'sourceSystem': source_system,
                'steward': steward,
                'businessImpact': business_impact,
                'regulatoryCompliance': regulatory,
                'dataQualityRisk': data_quality,
                'securityRisk': security,
                'systemComplexity': complexity,
                'recoveryDifficulty': recovery,
                'definition': editing_cde.get('definition', '') if editing_cde else '',
                'dataType': editing_cde.get('dataType', '') if editing_cde else '',
                'owner': editing_cde.get('owner', '') if editing_cde else '',
                'downstreamSystems': editing_cde.get('downstreamSystems', '') if editing_cde else '',
                'regulatory': editing_cde.get('regulatory', '') if editing_cde else '',
                'status': 'Active',
                'assessmentDate': datetime.now().strftime('%Y-%m-%d'),
                'notes': editing_cde.get('notes', '') if editing_cde else ''
            }
            
            if editing_id:
                st.session_state.cdes = [new_cde if c['id'] == editing_id else c for c in st.session_state.cdes]
                st.success(f"✅ Updated CDE: {name}")
            else:
                st.session_state.cdes.append(new_cde)
                st.success(f"✅ Added CDE: {name}")
            
            st.session_state.show_cde_form = False
            st.session_state.editing_cde_id = None
            st.rerun()
        
        if cancelled:
            st.session_state.show_cde_form = False
            st.session_state.editing_cde_id = None
            st.rerun()

# ============================================
# ACTION PLAN TAB WITH LLM
# ============================================
def render_action_plan():
    """Enhanced action plan with LLM integration"""
    st.markdown("### Action Plan")
    
    # Summary metrics
    total_actions = len(st.session_state.actions)
    complete = len([a for a in st.session_state.actions if a['status'] == 'Complete'])
    in_progress = len([a for a in st.session_state.actions if a['status'] == 'In Progress'])
    p1_open = len([a for a in st.session_state.actions if a['priority'] == 'P1' and a['status'] != 'Complete'])
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Actions", total_actions)
    with col2:
        st.metric("Complete", complete)
    with col3:
        st.metric("In Progress", in_progress)
    with col4:
        st.metric("P1 Open", p1_open)
    
    # Add Action button
    col1, col2 = st.columns([6, 1])
    with col2:
        if st.button("Add New Action", use_container_width=True, type="primary"):
            st.session_state.show_action_form = True
    
    # Action Form with LLM
    if st.session_state.get('show_action_form', False):
        render_action_form()
    
    # Actions Table
    if st.session_state.actions:
        action_data = []
        for action in st.session_state.actions:
            action_data.append({
                'ID': action['id'],
                'CDE': action['cdeName'],
                'Description': action['description'][:100] + '...' if len(action['description']) > 100 else action['description'],
                'Priority': action['priority'],
                'Status': action['status'],
                'Progress': f"{action['percentComplete']}%",
                'Owner': action['owner']
            })
        
        df = pd.DataFrame(action_data)
        st.table(df)
        
        # Manage actions
        st.markdown("---")
        st.markdown("##### Manage Actions")
        
        action_options = [f"{a['id']} - {a['description'][:30]}..." for a in st.session_state.actions]
        
        # Reduce width using columns
        act_col1, act_col2 = st.columns([1, 1])
        with act_col1:
            selected_action = st.selectbox("Select Action to manage:", ["Select"] + action_options)
        
        if selected_action and selected_action != "Select":
            action_id = selected_action.split(" - ")[0]
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Edit Action", type="primary"):
                    st.session_state.editing_action_id = action_id
                    st.session_state.show_action_form = True
                    st.rerun()
            with col2:
                if st.button("Delete Action", type="primary"):
                    st.session_state.actions = [a for a in st.session_state.actions if a['id'] != action_id]
                    st.success(f"Deleted {action_id}")
                    st.rerun()
    else:
        st.info("No actions found. Add a new action to get started.")

def render_action_form():
    """Enhanced Action form with LLM suggestions"""
    st.markdown("### Action Form with AI Assistance")
    
    editing_id = st.session_state.get('editing_action_id', None)
    editing_action = None
    if editing_id:
        editing_action = next((a for a in st.session_state.actions if a['id'] == editing_id), None)
    
    with st.form("action_form_llm"):
        # CDE Selection
        cde_options = [f"{c['id']} - {c['name']}" for c in st.session_state.cdes]
        
        col1, col2 = st.columns(2)
        
        with col1:
            selected_cde_idx = 0
            if editing_action:
                try:
                    selected_cde_idx = cde_options.index(f"{editing_action['cdeId']} - {editing_action['cdeName']}")
                except ValueError:
                    selected_cde_idx = 0
            
            selected_cde = st.selectbox(
                "Related CDE *",
                cde_options,
                index=selected_cde_idx
            )
        
        with col2:
            action_type = st.selectbox(
                "Action Type", 
                ACTION_TYPES,
                index=ACTION_TYPES.index(editing_action['type']) if editing_action else 0
            )
        
        # Action Name (what the user provides)
        action_name = st.text_input(
            "Action Name *",
            value=editing_action['description'].split('\n')[0][:100] if editing_action else "",
            placeholder="e.g., Implement encryption at rest, Add data quality checks, etc.",
            help="Provide a clear name for the action you want to take"
        )
        
        # Get selected CDE details
        cde_id = selected_cde.split(" - ")[0]
        selected_cde_obj = next((c for c in st.session_state.cdes if c['id'] == cde_id), None)
        
        # LLM Generation Section
        st.markdown("---")
        st.markdown("#### 🤖 AI-Powered Suggestions")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            st.info("💡 Click 'Generate AI Suggestions' to get intelligent action recommendations and priority based on the CDE risk profile.")
        with col2:
            generate_clicked = st.form_submit_button("🤖 Generate", use_container_width=True)
        
        # Initialize or get existing values
        if editing_action:
            if 'llm_description' not in st.session_state or st.session_state.get('editing_action_id') != editing_id:
                st.session_state.llm_description = editing_action['description']
                st.session_state.llm_priority = editing_action['priority']
        
        # Generate suggestions when button is clicked
        if generate_clicked and action_name and selected_cde_obj:
            with st.spinner("Analyzing..."):
                suggestions = generate_action_suggestions(action_name, selected_cde_obj)
                st.session_state.llm_description = suggestions['description']
                st.session_state.llm_priority = suggestions['priority']
                st.success("✅ AI suggestions generated! Review and edit below.")
                st.rerun()
        
        st.markdown("---")
        
        # Editable fields populated by LLM
        description = st.text_area(
            "Action Description/Suggestions *",
            value=st.session_state.get('llm_description', ''),
            height=150,
            help="Edit the AI-generated suggestions or write your own"
        )
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            priority = st.selectbox(
                "Priority",
                PRIORITIES,
                index=PRIORITIES.index(st.session_state.get('llm_priority', 'P2')),
                help="AI-suggested priority based on risk score"
            )
        
        with col2:
            status = st.selectbox(
                "Status",
                ACTION_STATUSES,
                index=ACTION_STATUSES.index(editing_action['status']) if editing_action else 0
            )
        
        with col3:
            owner = st.text_input(
                "Owner *",
                value=editing_action['owner'] if editing_action else ""
            )
        
        percent_complete = st.slider(
            "Percent Complete",
            0, 100,
            editing_action['percentComplete'] if editing_action else 0
        )
        
        # Save/Cancel buttons
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            submitted = st.form_submit_button("Save Action", use_container_width=True, type="primary")
        with col2:
            cancelled = st.form_submit_button("Cancel", use_container_width=True, type="secondary")
        
        if submitted and action_name and description and owner:
            cde_name = selected_cde.split(" - ")[1]
            
            # Get CDE risk tier
            risk_tier = 'Medium'
            if selected_cde_obj:
                risk_score = calculate_weighted_score(selected_cde_obj)
                risk_tier = get_risk_tier(risk_score)
            
            new_action = {
                'id': editing_id if editing_id else f"ACT-{str(len(st.session_state.actions) + 1).zfill(3)}",
                'cdeId': cde_id,
                'cdeName': cde_name,
                'riskTier': risk_tier,
                'description': description,
                'type': action_type,
                'priority': priority,
                'owner': owner,
                'status': status,
                'percentComplete': percent_complete,
                'dueDate': '',
                'notes': f"Action: {action_name}"
            }
            
            if editing_id:
                st.session_state.actions = [new_action if a['id'] == editing_id else a for a in st.session_state.actions]
            else:
                st.session_state.actions.append(new_action)
            
            # Clear LLM state
            st.session_state.llm_description = ""
            st.session_state.llm_priority = "P2"
            st.session_state.show_action_form = False
            st.session_state.editing_action_id = None
            st.success("✅ Action saved successfully!")
            st.rerun()
        
        if cancelled:
            st.session_state.llm_description = ""
            st.session_state.llm_priority = "P2"
            st.session_state.show_action_form = False
            st.session_state.editing_action_id = None
            st.rerun()

# ============================================
# ONBOARDING CDE TAB (Formerly Reference)
# ============================================
def render_identify_cde():
    st.header("Identify CDE")
    st.markdown("#### Select CDE to Evaluate")
    
    # Callback to sync selected CDE
    def sync_eval_selection():
        st.session_state.onboard_form_data['eval_selected_cde'] = st.session_state.eval_sel
    
    cde_options = [c['name'] for c in st.session_state.cdes]
    current_sel = st.session_state.onboard_form_data.get('eval_selected_cde')
    default_idx = cde_options.index(current_sel) if current_sel in cde_options else 0
    
    if cde_options:
        eval_col1, eval_col2 = st.columns([1, 1])
        with eval_col1:
            selected_cde_name = st.selectbox("Select CDE:", cde_options, index=default_idx, key="eval_sel", on_change=sync_eval_selection)
    else:
        st.warning("No existing CDEs found. Please add CDEs via 'Add CDE' or 'Upload from Excel' first.")
        selected_cde_name = None
        
    if selected_cde_name:
        current_cde = selected_cde_name.strip()
        
        st.markdown("---")
        
        # Card for current CDE
        card_col1, card_col2 = st.columns([1, 1])
        with card_col1:
            st.markdown(f"""
            <div class="navy-card">
                <h3 style="margin:0; font-family:'Inter',sans-serif;">Evaluating: {current_cde}</h3>
            </div>
            """, unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("##### ✅ Qualification Checklist")
            st.markdown("Check criteria that apply. **3+ criteria = Qualified CDE**")
            
            # Persistent Checklist logic
            if 'eval_checklist' not in st.session_state.onboard_form_data:
                st.session_state.onboard_form_data['eval_checklist'] = {}
            
            checklist_state = st.session_state.onboard_form_data['eval_checklist']
            if current_cde not in checklist_state:
                checklist_state[current_cde] = {}

            def sync_checklist(cde, crit_id, key):
                st.session_state.onboard_form_data['eval_checklist'][cde][crit_id] = st.session_state[key]

            criteria_met = 0
            for criterion in CRITERIA:
                crit_id = criterion['id']
                key = f"eval_{current_cde}_{crit_id}"
                
                # Restore from state
                saved_val = checklist_state[current_cde].get(crit_id, False)
                
                is_checked = st.checkbox(
                    f"**{criterion['name']}** - {criterion['desc']}",
                    value=saved_val,
                    key=key,
                    on_change=sync_checklist,
                    args=(current_cde, crit_id, key)
                )
                
                if is_checked:
                    criteria_met += 1
            
        with col2:
            st.markdown("##### Result")
            is_qualified = criteria_met >= 3
            
            bg_color = '#dcfce7' if is_qualified else '#f3f4f6'
            result_text = '✓ QUALIFIED' if is_qualified else 'Not Qualified'
            result_color = '#16a34a' if is_qualified else '#6b7280'
            
            st.markdown(f"""
            <div style="padding: 20px; border-radius: 8px; background-color: {bg_color}; text-align: center; border: 2px solid {result_color};">
                <div style="font-size: 40px; font-weight: bold; color: {result_color};">{criteria_met}/10</div>
                <div style="font-weight: bold; color: {result_color}; margin-top: 5px;">{result_text}</div>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("##### Actions")
            
            # Action Buttons
            if is_qualified:
                if st.button("Save / Update Qualification", use_container_width=True, type="primary"):
                    # Check if CDE already exists to update
                    existing_idx = next((i for i, c in enumerate(st.session_state.cdes) if c['name'] == current_cde), -1)
                    
                    if existing_idx >= 0:
                        # Update existing
                        st.session_state.cdes[existing_idx]['notes'] += f" | Re-qualified on {datetime.now().strftime('%Y-%m-%d')} ({criteria_met} criteria)"
                        st.session_state.cdes[existing_idx]['manual_qualified'] = True # 🔹 SET FLAG
                        st.session_state.cdes[existing_idx]['status'] = 'Qualified' # 🔹 SET STATUS
                        st.success(f"✅ Updated existing CDE: '{current_cde}'")
                    else:
                        # Add new (This path is unlikely if selecting from existing, but kept for robustness)
                        st.error("CDE not found in registry.")  
    else:
        if not st.session_state.cdes:
            st.info("No existing CDEs.")

def render_register_page():
    st.markdown("### CDE Register")
    
    # Custom CSS to mimic standard 'CDE Onboard' tabs while allowing programmatic switching
    st.markdown("""
        <style>
        .tab-spacer { border-bottom: 1px solid rgba(49, 51, 63, 0.1); margin-top: -15px; margin-bottom: 20px; }
        
        div[data-testid="column"] button {
            background: none !important;
            border: none !important;
            padding: 10px 0 !important;
            color: #31333f !important;
            font-weight: 400 !important;
            font-size: 16px !important;
            border-bottom: 2px solid transparent !important;
            border-radius: 0 !important;
            height: auto !important;
            min-height: 0 !important;
            line-height: 1.5 !important;
            box-shadow: none !important;
            transition: all 0.2s ease !important;
        }
        div[data-testid="column"] button:hover {
            color: #CC0000 !important;
            background: none !important;
        }
        /* Active Tab Highlight - Red Underline similar to Onboard */
        div[data-testid="column"] button[kind="primary"] {
            color: #CC0000 !important;
            border-bottom: 2px solid #CC0000 !important;
            font-weight: 600 !important;
        }
        </style>
    """, unsafe_allow_html=True)
    
    active_tab = st.session_state.get('cde_active_tab', 0)
    
    col_t1, col_t2, col_t3 = st.columns([1, 1, 4])
    with col_t1:
        if st.button("View CDEs", key="tab_0", use_container_width=True, type="primary" if active_tab == 0 else "secondary"):
            st.session_state.cde_active_tab = 0
            st.rerun()
    with col_t2:
        if st.button("Export CDEs", key="tab_1", use_container_width=True, type="primary" if active_tab == 1 else "secondary"):
            st.session_state.cde_active_tab = 1
            st.rerun()
    
    st.markdown("<div class='tab-spacer'></div>", unsafe_allow_html=True)
    
    if active_tab == 0:
        render_cde_view()
    else:
        if len(st.session_state.cdes) > 0:
            # Filter based on checkbox from View tab (Sync with render_cde_view)
            show_ai_only = st.session_state.get("show_ai_only", False)
            show_manual_only = st.session_state.get("show_manual_only", False)
            
            export_list = []
            for c in st.session_state.cdes:
                if show_ai_only and not c.get('ai_suggested', False):
                    continue
                if show_manual_only and not c.get('manual_qualified', False):
                    continue
                export_list.append(c)

            # --- Fabric Export Section ---
            st.markdown("##### Export to Microsoft Fabric")
            
            sql_end = st.session_state.connector_creds.get('fabric_sql_endpoint', '')
            sql_db = "w1" # Default
            
            col_end, col_db = st.columns([2, 1])
            with col_end:
                sql_end = st.text_input("SQL Endpoint (Connection String)", 
                                       value=sql_end, 
                                       type="password",
                                       help="SQL connection string from Warehouse settings.", 
                                       key="sql_end_tab2")
            with col_db:
                sql_db = st.text_input("Warehouse Name", value=sql_db, help="Enter the name of your Warehouse (e.g. 'w1')", key="sql_db_tab2")
            
            # Table Selection Row
            tcol1, tcol2 = st.columns([2, 3])
            with tcol1:
                table_mode = st.selectbox("Target Table", ["Existing Table", "New Table"], key="sync_mode_tab2")
                create_needed = (table_mode == "New Table")
            
            with tcol2:
                if table_mode == "Existing Table":
                    # Auto-fetch tables if list is empty
                    if sql_end and not st.session_state.get('fabric_tables'):
                        with st.spinner("Fetching tables..."):
                            try:
                                from backend.fabric_connector import FabricConnector
                                creds = st.session_state.connector_creds
                                connector = FabricConnector(creds.get('fabric_tenant_id', ''), creds.get('fabric_client_id', ''), creds.get('fabric_client_secret', ''))
                                st.session_state.fabric_tables = connector.list_tables(sql_end, database_name=sql_db)
                                st.rerun()
                            except Exception as e:
                                st.error(f"⚠️ Could not load tables: {str(e)}")
                                sql_tab = st.text_input("Target Table Name", value="", help="Enter table name manually.", key="sync_tab_manual_tab2")
                    
                    if st.session_state.get('fabric_tables'):
                        sql_tab = st.selectbox("Select Existing Table", st.session_state.fabric_tables, key="sync_tab_list_tab2")
                    else:
                        sql_tab = st.text_input("Target Table Name", value="", help="Enter table name manually.", key="sync_tab_manual_fallback_tab2")
                else:
                    sql_tab = st.text_input("New Table Name", value="", key="sync_tab_new_tab2")
            
            # Place Export button in columns to keep it short and on the left
            btn_col1, btn_col2 = st.columns([2, 10])
            with btn_col1:
                if st.button("Export", type="primary", use_container_width=True, key="fabric_export_btn_tab2"):
                    if not sql_end or not sql_tab:
                        st.error("Please provide SQL Endpoint and Table Name.")
                    else:
                        with st.spinner("Exporting..."):
                            try:
                                from backend.fabric_connector import FabricConnector
                                creds = st.session_state.connector_creds
                                connector = FabricConnector(
                                    creds.get('fabric_tenant_id', ''),
                                    creds.get('fabric_client_id', ''),
                                    creds.get('fabric_client_secret', '')
                                )
                                
                                conn = connector.get_sql_connection(sql_end, database_name=sql_db)
                                df_to_sync = pd.DataFrame(export_list) # Use the filtered export_list
                                success, msg = connector.sync_to_fabric(df_to_sync, sql_end, sql_tab, database_name=sql_db, create_if_not_exists=create_needed)
                                
                                if success:
                                    st.success("Exported")
                                    st.session_state.connector_creds['fabric_sql_endpoint'] = sql_end
                                else:
                                    if "denied" in msg.lower() or "368" in msg:
                                        st.error("Permission Denied (Read-Only Endpoint)")
                                        st.warning("Lakehouse SQL Endpoints are Read-Only. Please use a Warehouse endpoint (usually starts with data-) to export data!")
                                    else:
                                        st.error(msg)
                            except Exception as e:
                                st.error(str(e))
        else:
            st.info("No CDEs to export.")


# ============================================
# FOOTER
# ============================================
def render_footer():
    st.markdown(
    """
    <div class="ilink-footer">
        Powered by <strong style="color: #CC0000;">iLink</strong> <strong>Catalyst</strong> &nbsp;|&nbsp; CDE Catalyst v2.0 with AI &nbsp;&nbsp; <span style="color: #CC0000;">◆</span> &nbsp; <strong>iLink Digital</strong>
    </div>
    """,
    unsafe_allow_html=True
)

# ============================================
# LOGIN PAGE
# ============================================
def render_login():
    # Styling for the form to look like a card
    st.markdown("""
    <style>
        [data-testid="stForm"] {
            background-color: white;
            border: 1px solid #e7e5e4;
            border-top: 5px solid #CC0000; /* Red Highlight */
            box-shadow: 0 10px 30px rgba(0,0,0,0.05);
            padding: 32px;
            border-radius: 2px;
        }
        [data-testid="stForm"] input {
            background-color: #fafafa; /* Light Input bg */
            border: 1px solid #e5e7eb;
            color: #1c1917;
        }
    </style>
    """, unsafe_allow_html=True)

    # Centered container
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("<br>" * 4, unsafe_allow_html=True)
        
        with st.form("login_form"):
            # Load Logo
            # Load Logo
            try:
                import base64
                with open("assets/login_icon.png", "rb") as f:
                    data = base64.b64encode(f.read()).decode("utf-8")
                logo_html = f'<img src="data:image/png;base64,{data}" style="max-width: 120px; margin-bottom: 24px;">'
            except:
                logo_html = '<div style="font-size: 64px; margin-bottom: 20px;">🛡️</div>'
                
            st.markdown(f"""
            <div style="text-align: center; margin-bottom: 24px;">
                {logo_html}
                <h1 style="font-family: 'Oswald', sans-serif; color: #1c1917; margin: 0 0 8px 0; font-size: 32px; letter-spacing: 0.05em; text-transform: uppercase;">CDE CATALYST</h1>
                <h2 style="font-family: 'Inter', sans-serif; color: #78716c; margin: 0; font-size: 16px; font-weight: 500; letter-spacing: 0.05em; text-transform: uppercase;">WELCOME BACK</h2>
                <p style="font-family: 'Inter', sans-serif; color: #d6d3d1; font-size: 13px; margin-top: 4px;">Please sign in to continue</p>
            </div>
            """, unsafe_allow_html=True)
            
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            
            # Credential Hint
            st.markdown("""
                <div style="text-align: center; margin-top: 10px; margin-bottom: 20px; color: #a8a29e; font-size: 12px; font-family: 'Inter', sans-serif;">
                    <span style="background: #f5f5f4; padding: 4px 8px; border-radius: 4px;">Hint: admin / admin</span>
                </div>
            """, unsafe_allow_html=True)
            
            submit = st.form_submit_button("SIGN IN", use_container_width=True)
            
            if submit:
                if username == "admin" and password == "admin":
                    st.session_state.logged_in = True
                    st.rerun()
                else:
                    st.error("Invalid credentials")

# ============================================
# MAIN APP
# ============================================
def main():
    # 🔒 CRITICAL: Initialize session state ONCE and protect it
    init_session_state()
    
    # Check login state
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
        
    if not st.session_state.logged_in:
        render_login()
        return

    # 🛡️ SAFEGUARD: Verify CDE list integrity on every run
    if 'cdes' not in st.session_state:
        st.error("⚠️ CRITICAL: CDEs list was deleted from session state!")
        st.session_state.cdes = []
    
    # Initialize settings state
    if 'show_settings' not in st.session_state:
        st.session_state.show_settings = False
    
    # Initialize selected_tab in session state
    if 'selected_tab' not in st.session_state:
        st.session_state.selected_tab = "CDE Onboard"
    
    # Render sidebar with badge, stepper, and logo
    render_sidebar()
    
    # 🎯 CONTENT BASED ON SIDEBAR NAVIGATION
    selected = st.session_state.selected_tab
    
    if selected == "CDE Onboard":
        render_cde_register()
    elif selected == "CDE Register":
        render_register_page()
    elif selected == "Action Plan":
        render_action_plan()
    elif selected == "Dashboard":
        render_dashboard()
    
    render_footer()


if __name__ == "__main__":
    main()
