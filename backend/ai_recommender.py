import streamlit as st
import json
import time
import pandas as pd
import os
from dotenv import load_dotenv
from google import genai

# Load environment variables from .env file (if it exists)
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
load_dotenv(env_path)

def get_gemini_client():
    """Initialize Gemini client with API key from environment or streamlit secrets"""
    try:
        # Priority 1: Environment Variable (.env or system)
        api_key = os.getenv("GEMINI_API_KEY")
        # st.write(f"DEBUG: Key from env: {api_key[:5] if api_key else 'None'}...")
        
        # Priority 2: Streamlit Secrets (fallback if env not set OR is the placeholder)
        if not api_key or api_key == "YOUR_GEMINI_API_KEY_HERE":
            # st.write("DEBUG: Falling back to st.secrets")
            api_key = st.secrets.get("GEMINI_API_KEY")
            
        if not api_key or api_key == "YOUR_GEMINI_API_KEY_HERE":
            st.error(f"⚠️ GEMINI_API_KEY not found. App looked at: {env_path}")
            # st.write(f"DEBUG: env_path exists: {os.path.exists(env_path)}")
            return None
            
        return genai.Client(api_key=api_key)
    except Exception as e:
        st.error(f"Error initializing Gemini client: {str(e)}")
        return None

# ============================================
# AI RECOMMENDATION LOGIC
# ============================================

def generate_cde_suggestions(business_requirement, industry="General", file_columns=None):
    """Generate CDE suggestions using Gemini based on business requirement, industry, and optional file schema"""
    client = get_gemini_client()
    
    if not client:
        st.warning("⚠️ Gemini API key not configured. Please add your GEMINI_API_KEY to .env or .streamlit/secrets.toml")
        return []
    
    # Construct Contextual Prompt
    context_part = f"Industry Context: {industry}\n"
    if file_columns:
        context_part += f"Target Dataset Columns: {', '.join(file_columns)}\n"
        task_instruction = "Task: Analyze the provided dataset columns and the business requirement. Identify which of these columns (or other missing elements) are Critical Data Elements."
    else:
        task_instruction = "Task: Identify 3-5 potential CDEs that are relevant to this requirement."

    prompt = f"""You are a data governance expert in the {industry} industry. 
    
    **Context:**
    {context_part}
    
    **Business Requirement:**
    "{business_requirement}"
    
    **{task_instruction}**
    
    For each CDE, provide:
    1. Name (Use exact column name if from dataset)
    2. Domain (Must be one of: Retail, Healthcare, Finance, Manufacturing, Energy, Government, Insurance, Other. Do NOT use 'Detected from File')
    3. Definition (Brief description)
    4. Rationale (Why is this critical?)
    
    **Format:**
    Respond ONLY with a JSON array of objects. Example:
    [
        {{
            "name": "CDE Name",
            "domain": "Domain Name",
            "definition": "Description...",
            "rationale": "Reasoning..."
        }}
    ]
    """
    
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt
        )
        
        response_text = response.text
        # Clean up code blocks if present
        if "```json" in response_text:
            response_text = response_text.split("```json")[1].split("```")[0].strip()
        elif "```" in response_text:
            response_text = response_text.split("```")[1].split("```")[0].strip()
            
        result = json.loads(response_text)
        return result
    except Exception as e:
        st.error(f"❌ Error generating AI suggestions: {str(e)}")
        return []

def recommend_cdes_from_columns(table_name, columns, industry="General"):
    """Specifically recommend CDEs based on a table schema (columns)"""
    client = get_gemini_client()
    if not client: return []
    
    prompt = f"""You are a data governance expert in the {industry} industry. 
    Analyze the schema for table '{table_name}' with columns: {', '.join(columns)}.
    Identify 3-5 Critical Data Elements (CDEs) from these columns.
    For each, provide: name (exact column name), domain, definition, and rationale.
    Respond ONLY with a JSON array."""
    
    try:
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        text = response.text
        if "```json" in text: text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text: text = text.split("```")[1].split("```")[0].strip()
        return json.loads(text)
    except Exception as e:
        st.error(f"❌ AI Error: {str(e)}")
        return []

class AIRecommender:
    def recommend_cdes_from_columns(self, table_name, columns, industry="General"):
        return recommend_cdes_from_columns(table_name, columns, industry)

def render_ai_recommend():
    """Render AI CDE Recommendation Tab"""
    # Clean UI styling - no negative margins to avoid overlaps
    st.markdown("""
        <style>
        .ai-config-container {
            background-color: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            border: 1px solid #dee2e6;
            margin-top: 10px;
            margin-bottom: 20px;
        }
        /* Style adjustments for labels */
        .stSelectbox label, .stTextInput label {
            font-weight: 600 !important;
            margin-bottom: 4px !important;
        }
        </style>
    """, unsafe_allow_html=True)

    # Use HTML for header to avoid anchor links (the link icon)
    st.markdown('<h3 style="margin-bottom: 0px;">AI CDE Recommender</h3>', unsafe_allow_html=True)
    st.markdown('<div style="color: #666; margin-bottom: 20px;">Identify Critical Data Elements from your data source using AI analysis.</div>', unsafe_allow_html=True)
    
    col_ind, col_conn = st.columns(2)
    
    with col_ind:
        st.markdown("**1. Industry Domain**")
        selected_industry = st.selectbox("Industry", 
                                       ["General", "Finance / Banking", "Healthcare", "Retail / E-Commerce", "Manufacturing", "Energy / Utilities", "Insurance"], 
                                       key="ai_selected_industry")
        
    with col_conn:
        st.markdown("**2. Data Source**")
        connector_type = st.selectbox("Connector", ["Excel", "Microsoft Fabric"], key="ai_connector_type")
        
        # Reset discovery when connector changes
        if 'prev_ai_connector' not in st.session_state or st.session_state.prev_ai_connector != connector_type:
            st.session_state.ai_discovered_cols = []
            st.session_state.prev_ai_connector = connector_type

    # Connector specific inputs
    file_columns = []
    fabric_table = None
    
    if connector_type == "Excel":
        uploaded_file = st.file_uploader("Upload Excel / CSV file", type=["csv", "xlsx"])
        if uploaded_file:
            st.session_state.ai_excel_filename = uploaded_file.name
            try:
                if uploaded_file.name.endswith('.csv'):
                    df_preview = pd.read_csv(uploaded_file, nrows=5)
                else:
                    df_preview = pd.read_excel(uploaded_file, nrows=5)
                file_columns = df_preview.columns.tolist()
                st.session_state.ai_discovered_cols = file_columns
                st.success(f"Loaded {len(file_columns)} columns.")
            except Exception as e:
                st.error(f"Error reading file: {str(e)}")
    else:
        # Fabric Connector UI - Flexible Auth Roles
        f_sql = st.text_input("SQL Endpoint / Connection String", 
                             value=st.session_state.connector_creds.get('fabric_sql_endpoint', ''), 
                             type="password",
                             key="ai_f_sql_input",
                             placeholder="xxxxxxxx.datawarehouse.fabric.microsoft.com")
        
        # Reset discovery when endpoint changes
        if 'prev_f_sql' not in st.session_state or st.session_state.prev_f_sql != f_sql:
            st.session_state.ai_fabric_tables = []
            st.session_state.prev_f_sql = f_sql
            st.session_state.ai_fabric_error = None

        # Configuration Section
        st.write("---")
        auth_mode = st.radio("Authentication Mode", 
                            ["Interactive Login (Standard)", "Email & Password (AAD)", "Service Principal (Automation/Cloud)"], 
                            index=1, horizontal=True)
        
        creds = st.session_state.connector_creds
        
        if auth_mode == "Service Principal (Automation/Cloud)":
            st.info("💡 Service Principal is required for Streamlit Cloud automation.")
            col1, col2, col3 = st.columns(3)
            with col1: creds['fabric_tenant_id'] = st.text_input("Tenant ID", value=creds.get('fabric_tenant_id', ''), key="ai_f_tenant_fl")
            with col2: creds['fabric_client_id'] = st.text_input("Client ID", value=creds.get('fabric_client_id', ''), key="ai_f_client_fl")
            with col3: creds['fabric_client_secret'] = st.text_input("Client Secret", value=creds.get('fabric_client_secret', ''), type="password", key="ai_f_secret_fl")
        elif auth_mode == "Email & Password (AAD)":
            st.warning("⚠️ **MFA Note**: If your account uses an Authenticator app/code, this mode will NOT work. Please use **Interactive Login** instead.")
            col1, col2 = st.columns(2)
            with col1: creds['fabric_email'] = st.text_input("Email", value=creds.get('fabric_email', ''), placeholder="user@domain.com", key="ai_f_email_aad")
            with col2: creds['fabric_password'] = st.text_input("Password", value=creds.get('fabric_password', ''), type="password", key="ai_f_pwd_aad")
        else:
            st.info("💡 **Recommended for MFA**: This will open a Microsoft window for your email, password, and Authenticator code.")
            creds['fabric_email'] = st.text_input("Email (Optional Hint)", value=creds.get('fabric_email', ''), placeholder="user@domain.com", key="ai_f_email_fl")

        # Trigger Discovery
        if st.button("🔍 Discover Tables", type="primary", use_container_width=True):
            if not f_sql:
                st.error("Please provide a SQL Endpoint first.")
            else:
                with st.spinner("Connecting to Fabric... Please check for a login popup if using Interactive mode."):
                    try:
                        from backend.fabric_connector import FabricConnector
                        st.session_state.ai_fabric_error = None
                        
                        # Route credentials based on mode
                        if auth_mode == "Service Principal (Automation/Cloud)":
                            t_id = creds.get('fabric_tenant_id', '')
                            c_id = creds.get('fabric_client_id', '')
                            c_sec = creds.get('fabric_client_secret', '')
                        elif auth_mode == "Email & Password (AAD)":
                            t_id = ""
                            c_id = creds.get('fabric_email', '')
                            c_sec = f"AAD_PWD:{creds.get('fabric_password', '')}"
                        else:
                            t_id = ""
                            c_id = creds.get('fabric_email', '') 
                            c_sec = ""
                        
                        connector = FabricConnector(t_id, c_id, c_sec)
                        # Explicitly mention timeout in the UI log or keep it in the backend logic
                        tables = connector.list_tables(f_sql, database_name=None) # Passing None to use default/connection string database
                        st.session_state.ai_fabric_tables = tables
                        if not tables:
                            st.session_state.ai_fabric_error = "No tables found or access denied. Check your permissions in the Fabric Workspace."
                        else:
                            st.success(f"Successfully discovered {len(tables)} tables!")
                        st.rerun()
                    except Exception as e:
                        st.session_state.ai_fabric_error = f"Connection failed: {str(e)}"
                        st.rerun()
                        st.session_state.ai_fabric_tables = tables
                        if not tables:
                            st.session_state.ai_fabric_error = "No tables found or access denied."
                        else:
                            st.success(f"Found {len(tables)} tables!")
                        st.rerun()
                    except Exception as e:
                        st.session_state.ai_fabric_error = f"Connection failed: {str(e)}"
                        st.rerun()

        # Display Error if any
        if st.session_state.get('ai_fabric_error'):
            st.error(st.session_state.ai_fabric_error)
            if st.button("Clear Error"):
                st.session_state.ai_fabric_error = None
                st.rerun()

        # Conditional Display: Dropdown vs Text Input
        fabric_tables = st.session_state.get('ai_fabric_tables', [])
        if fabric_tables:
            fabric_table = st.selectbox("Select Table", ["--- Select Table ---"] + fabric_tables, key="ai_f_tab_sel_ref")
            if fabric_table == "--- Select Table ---": fabric_table = None
        else:
            fabric_table = st.text_input("Table Name", placeholder="e.g. Sales_Transactions", key="ai_f_tab_text_ref")

        # Live discovery of Fabric Columns
        if fabric_table and ('prev_ai_f_tab' not in st.session_state or st.session_state.prev_ai_f_tab != fabric_table):
            with st.spinner(f"Discovering attributes for '{fabric_table}'..."):
                try:
                    from backend.fabric_connector import FabricConnector
                    creds = st.session_state.connector_creds
                    connector = FabricConnector(creds.get('fabric_tenant_id', ''), creds.get('fabric_client_id', ''), creds.get('fabric_client_secret', ''))
                    schema = connector.fetch_table_schema(f_sql, fabric_table, database_name="w1")
                    if schema:
                        st.session_state.ai_discovered_cols = [c['name'] for c in schema]
                        st.session_state.prev_ai_f_tab = fabric_table
                    else:
                        st.session_state.ai_discovered_cols = []
                except Exception:
                    st.session_state.ai_discovered_cols = []

    # Business Requirement Input
    requirement = st.text_area("Business Requirement / Context", 
                              height=100, 
                              placeholder="Example: We need to comply with GDPR for our European customer data...",
                              key="ai_requirement")
    
    if st.button("Analyze & Recommend CDEs", type="primary"):
        cols_to_analyze = file_columns
        
        # Handle Fabric Fetching if needed
        if connector_type == "Microsoft Fabric":
            if not f_sql or not fabric_table:
                st.error("Please provide both SQL Endpoint and Table Name.")
                return
            
            with st.spinner("Analyzing..."):
                try:
                    from backend.fabric_connector import FabricConnector
                    creds = st.session_state.connector_creds
                    connector = FabricConnector(
                        creds.get('fabric_tenant_id', ''),
                        creds.get('fabric_client_id', ''),
                        creds.get('fabric_client_secret', '')
                    )
                    schema = connector.fetch_table_schema(f_sql, fabric_table, database_name="w1")
                    if schema:
                        cols_to_analyze = [c['name'] for c in schema]
                    else:
                        st.error("Could not fetch table schema.")
                        return
                except Exception as e:
                    st.error(f"Fabric Error: {str(e)}")
                    return
        
        if not requirement and not cols_to_analyze:
            st.warning("Please provide context (requirement or schema) for analysis.")
        else:
            # Main Analysis Logic
            with st.spinner("Analyzing..."):
                suggestions = generate_cde_suggestions(requirement, selected_industry, cols_to_analyze)
                st.session_state.ai_cde_suggestions = suggestions
                # Store columns for reference display
                st.session_state.ai_discovered_cols = cols_to_analyze
                if suggestions:
                    st.success(f"Analysis complete. Identified {len(suggestions)} potential CDEs.")
                else:
                    st.warning("No CDEs identified based on the provided context.")
                
    # --- Live Attribute Display (Moved below Analyze button) ---
    if 'ai_discovered_cols' in st.session_state and st.session_state.ai_discovered_cols:
        st.markdown(f"**Discovered Attributes ({len(st.session_state.ai_discovered_cols)} found):**")
        cols_html = "".join([f"<span style='background:#f1f5f9; color:#475569; padding:2px 10px; border-radius:12px; margin-right:5px; margin-bottom:5px; display:inline-block; font-size:12px; border:1px solid #e2e8f0;'>{col}</span>" for col in st.session_state.ai_discovered_cols])
        st.markdown(f"<div>{cols_html}</div><div style='margin-bottom:15px;'></div>", unsafe_allow_html=True)
                
    # Display Results
    if 'ai_cde_suggestions' in st.session_state and st.session_state.ai_cde_suggestions:
        st.divider()
        
        st.subheader(f"Recommended CDEs ({len(st.session_state.ai_cde_suggestions)})")
        
        # Get existing CDE names for checking status
        existing_names = [cde['name'].lower() for cde in st.session_state.cdes]
        
        for i, item in enumerate(st.session_state.ai_cde_suggestions):
            with st.container():
                api_col1, api_col2 = st.columns([4, 1])
                with api_col1:
                    st.markdown(f"**{item.get('name', 'N/A')}** <span style='background:#f3f4f6; padding:2px 8px; border-radius:10px; font-size:12px;'>{item.get('domain', 'Reference')}</span>", unsafe_allow_html=True)
                    st.markdown(f"_{item.get('definition', 'No definition provided')}_")
                    st.markdown(f"**Why Critical:** {item.get('rationale', item.get('reasoning', 'Not provided'))}")
                with api_col2:
                    # Check if already in registry
                    item_name = item.get('name', 'N/A')
                    if item_name.lower() in existing_names:
                        st.button("✅ Added", key=f"added_btn_{i}", disabled=True)
                    else:
                        if st.button("Add to Register", key=f"add_ai_cde_{i}", type="primary"):
                            # Dynamic Source Identification
                            source_system = "AI Recommended"
                            if connector_type == "Excel":
                                source_system = "Excel Source"
                            elif connector_type == "Microsoft Fabric":
                                source_system = "Microsoft Fabric"

                            # Add to CDE list
                            new_cde = {
                                "id": f"CDE-{len(st.session_state.cdes) + 100}", # Simple ID gen
                                "name": item.get('name', 'N/A'),
                                "domain": item.get('domain', 'Reference'),
                                "definition": item.get('definition', 'No definition provided'),
                                "sourceSystem": source_system,
                                "ai_suggested": True, # Flag as AI
                                "status": "Qualified", # Auto-qualified by AI
                                "businessImpact": 3, # Default
                                "regulatoryCompliance": 3,
                                "dataQualityRisk": 3,
                                "securityRisk": 3,
                                "systemComplexity": 3,
                                "recoveryDifficulty": 3,
                                "notes": f"Recommended by Gemini AI from {source_system}. Context: {requirement[:50]}..."
                            }
                            st.session_state.cdes.append(new_cde)
                            time.sleep(0.5)
                            st.rerun()
                st.divider()
