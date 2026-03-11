import streamlit as st
import json
import time
import pandas as pd
import os
from dotenv import load_dotenv
from google import genai

# Load environment variables from .env file
# We use an absolute path relative to this file's directory
from pathlib import Path
dotenv_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=dotenv_path)

def get_ai_client(direct_key=None):
    """Initialize AI client with API key from environment, secrets, or direct input"""
    try:
        api_key = direct_key
        
        # Priority 1: Environment Variable (.env or system)
        if not api_key:
            api_key = os.getenv("AI_API_KEY")
        
        # Priority 2: Manual .env parse fallback (BOM-aware)
        if (not api_key or api_key == "YOUR_API_KEY_HERE") and dotenv_path.exists():
            try:
                # utf-8-sig handles the Byte Order Mark (BOM) automatically
                content = dotenv_path.read_text(encoding="utf-8-sig")
                for line in content.splitlines():
                    if 'AI_API_KEY=' in line:
                        api_key = line.split('=', 1)[1].strip().strip('"').strip("'")
                        os.environ["AI_API_KEY"] = api_key 
                        break
            except Exception:
                pass

        # Priority 3: Streamlit Secrets
        if not api_key or api_key == "YOUR_API_KEY_HERE":
            api_key = st.secrets.get("AI_API_KEY")
            
        if not api_key or api_key == "YOUR_API_KEY_HERE":
            st.error(f"⚠️ AI_API_KEY not found in environment or secrets.")
            st.info(f"💡 The app tried to load from: `{dotenv_path.absolute()}`")
            if dotenv_path.exists():
                st.write("✅ .env file exists.")
                st.write(f"ℹ️ File size: {dotenv_path.stat().st_size} bytes")
                try:
                    with open(dotenv_path, 'rb') as f:
                        header = f.read(10)
                        st.write(f"🔍 File header bytes: `{header.hex(' ')}`")
                except: pass
            return None
            
        return genai.Client(api_key=api_key)
    except Exception as e:
        st.error(f"Error initializing AI client: {str(e)}")
        return None

# ============================================
# AI RECOMMENDATION LOGIC
# ============================================

def generate_cde_suggestions(business_requirement, industry="General", file_columns=None, direct_key=None):
    """Generate CDE suggestions using AI based on business requirement, industry, and optional file schema"""
    client = get_ai_client(direct_key=direct_key)
    
    if not client:
        st.warning("⚠️ API key not configured. Please add your AI_API_KEY to .env or .streamlit/secrets.toml")
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
            model="gemini-2.0-flash",
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
    client = get_ai_client()
    if not client: return []
    
    prompt = f"""You are a data governance expert in the {industry} industry. 
    Analyze the schema for table '{table_name}' with columns: {', '.join(columns)}.
    Identify 3-5 Critical Data Elements (CDEs) from these columns.
    For each, provide: name (exact column name), domain, definition, and rationale.
    Respond ONLY with a JSON array."""
    
    try:
        # Check if client exists
        if not client:
            st.error("❌ AI Client not initialized. Please check your API key.")
            return []
            
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        text = response.text
        if "```json" in text: text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text: text = text.split("```")[1].split("```")[0].strip()
        return json.loads(text)
    except Exception as e:
        st.error(f"❌ AI Prediction Error: {str(e)}")
        if "403" in str(e):
            st.info("💡 Your API key might be restricted or restricted by region. Try creating a new one in Google AI Studio.")
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
    
    # --- Core Connection Info (Visible on Surface) ---
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
        # Fabric Connector UI - Core Fields
        col_sql, col_db = st.columns([2, 1])
        with col_sql:
            f_sql = st.text_input("SQL Endpoint / Connection String", 
                                 value=st.session_state.connector_creds.get('fabric_sql_endpoint', ''), 
                                 type="password",
                                 key="ai_f_sql_input",
                                 placeholder="xxxxxxxx.datawarehouse.fabric.microsoft.com")
        with col_db:
            f_db = st.text_input("Warehouse / Database Name", 
                                value=st.session_state.connector_creds.get('fabric_database', ''), 
                                placeholder="e.g. w1",
                                key="ai_f_db_input")
        
        # Reset strings for change detection
        if ('prev_f_sql' not in st.session_state or st.session_state.prev_f_sql != f_sql or 
            'prev_f_db' not in st.session_state or st.session_state.prev_f_db != f_db):
            st.session_state.ai_fabric_tables = []
            st.session_state.prev_f_sql = f_sql
            st.session_state.prev_f_db = f_db
            st.session_state.connector_creds['fabric_sql_endpoint'] = f_sql
            st.session_state.connector_creds['fabric_database'] = f_db

        # --- Minimal Setup & Authentication Expander (Arrow only) ---
        with st.expander("🔽 Connection & Setup Details"):
            auth_mode = st.radio("Authentication Mode", 
                                ["Interactive Login (Standard)", "Email & Password (AAD)", "Service Principal (Automation/Cloud)"], 
                                index=1, horizontal=True, key="ai_auth_mode_radio")
            
            creds = st.session_state.connector_creds
            st.caption("ℹ️ Fabric Endpoints use the SQL Server protocol.")

            if auth_mode == "Service Principal (Automation/Cloud)":
                col1, col2, col3 = st.columns(3)
                with col1: creds['fabric_tenant_id'] = st.text_input("Tenant ID", value=creds.get('fabric_tenant_id', ''), key="ai_f_tenant_fl")
                with col2: creds['fabric_client_id'] = st.text_input("Client ID", value=creds.get('fabric_client_id', ''), key="ai_f_client_fl")
                with col3: creds['fabric_client_secret'] = st.text_input("Client Secret", value=creds.get('fabric_client_secret', ''), type="password", key="ai_f_secret_fl")
            elif auth_mode == "Email & Password (AAD)":
                col1, col2 = st.columns(2)
                with col1: creds['fabric_email'] = st.text_input("Email", value=creds.get('fabric_email', ''), placeholder="user@domain.com", key="ai_f_email_aad")
                with col2: creds['fabric_password'] = st.text_input("Password", value=creds.get('fabric_password', ''), type="password", key="ai_f_pwd_aad")
            else:
                creds['fabric_email'] = st.text_input("Email (Optional Hint)", value=creds.get('fabric_email', ''), placeholder="user@domain.com", key="ai_f_email_fl")
                
                with st.expander("⚙️ Advanced Authentication Settings"):
                    creds['fabric_custom_client_id'] = st.text_input("Custom Client ID", 
                                                                 value=creds.get('fabric_custom_client_id', '1950a258-227b-4e31-a9cf-717495945fc2'), 
                                                                 key="ai_f_custom_cid")
                    creds['fabric_tenant_id'] = st.text_input("Tenant ID", 
                                                          value=creds.get('fabric_tenant_id', ''), 
                                                          key="ai_f_tenant_fl_adv")

            # --- Device Code Flow Step-by-Step UI ---
            if auth_mode == "Interactive Login (Standard)":
                col_code, col_verify = st.columns(2)
                with col_code:
                    if st.button("🔑 1. Get Login Code", use_container_width=True):
                        try:
                            import msal
                            client_id = creds.get('fabric_custom_client_id', '').strip() or "1950a258-227b-4e31-a9cf-717495945fc2"
                            tenant = creds.get('fabric_tenant_id', '').strip() or "organizations"
                            app = msal.PublicClientApplication(client_id, authority=f"https://login.microsoftonline.com/{tenant}")
                            flow = app.initiate_device_flow(scopes=["https://database.windows.net/.default"])
                            if "user_code" in flow:
                                st.session_state.ai_f_flow = flow
                                st.rerun()
                            else:
                                st.error(f"Flow error: {flow.get('error_description', 'Error initiating flow.')}")
                        except Exception as e:
                            st.error(f"Init error: {str(e)}")
                
                with col_verify:
                    if st.button("✅ 2. Verify & Connect", type="primary", use_container_width=True):
                        if 'ai_f_flow' not in st.session_state:
                            st.warning("Please click 'Get Login Code' first.")
                        else:
                            with st.spinner("Verifying..."):
                                try:
                                    import msal
                                    from backend.fabric_connector import FabricConnector
                                    client_id = creds.get('fabric_custom_client_id', '').strip() or "1950a258-227b-4e31-a9cf-717495945fc2"
                                    tenant = creds.get('fabric_tenant_id', '').strip() or "organizations"
                                    app = msal.PublicClientApplication(client_id, authority=f"https://login.microsoftonline.com/{tenant}")
                                    result = app.acquire_token_by_device_flow(st.session_state.ai_f_flow)
                                    if "access_token" in result:
                                        st.session_state.ai_f_token = result["access_token"]
                                        connector = FabricConnector("", "", "")
                                        tables = connector.list_tables(f_sql, database_name=f_db, access_token=st.session_state.ai_f_token)
                                        st.session_state.ai_fabric_tables = tables
                                        st.session_state.ai_fabric_error = None
                                        del st.session_state.ai_f_flow
                                        st.success(f"Connected! Discovered {len(tables)} tables.")
                                        st.rerun()
                                    else:
                                        st.error("Login not verified.")
                                except Exception as e:
                                    st.error(f"Connect error: {str(e)}")

            if 'ai_f_flow' in st.session_state and auth_mode == "Interactive Login (Standard)":
                flow = st.session_state.ai_f_flow
                st.markdown(f"""
                <div style="background-color: #f0f2f6; padding: 15px; border-radius: 10px; border-left: 5px solid #ff4b4b;">
                    <p>1. Go to: <a href="{flow['verification_uri']}" target="_blank">{flow['verification_uri']}</a></p>
                    <p>2. Enter Code: <span style="font-family: monospace; font-size: 1.2em; color: #ff4b4b;">{flow['user_code']}</span></p>
                </div>
                """, unsafe_allow_html=True)

            # Traditional Discover Button
            if auth_mode != "Interactive Login (Standard)":
                if st.button("🔍 Discover Tables", type="primary", use_container_width=True):
                    with st.spinner("Connecting..."):
                        try:
                            from backend.fabric_connector import FabricConnector
                            if auth_mode == "Service Principal (Automation/Cloud)":
                                t_id, c_id, c_sec = creds.get('fabric_tenant_id', ''), creds.get('fabric_client_id', ''), creds.get('fabric_client_secret', '')
                            else:
                                t_id, c_id, c_sec = "", creds.get('fabric_email', ''), f"AAD_PWD:{creds.get('fabric_password', '')}"
                            
                            connector = FabricConnector(t_id, c_id, c_sec)
                            tables = connector.list_tables(f_sql, database_name=f_db) 
                            st.session_state.ai_fabric_tables = tables
                            if tables: st.success(f"Discovered {len(tables)} tables!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Connection failed: {str(e)}")

            # Troubleshooting & Error Display
            with st.expander("🛠️ Troubleshooting"):
                if st.button("🔌 Run Environment Check"):
                    try:
                        import pyodbc
                        st.write("**ODBC Drivers:**", pyodbc.drivers())
                    except Exception as ex: st.error(str(ex))

            # Table Selection inside expander (if discovered)
            fabric_tables = st.session_state.get('ai_fabric_tables', [])
            if fabric_tables:
                fabric_table = st.selectbox("Select Table", ["--- Select Table ---"] + fabric_tables, key="ai_f_tab_sel_ref")
                if fabric_table == "--- Select Table ---": fabric_table = None
            else:
                fabric_table = st.text_input("Table Name", placeholder="e.g. Sales_Transactions", key="ai_f_tab_text_ref")

            # Column discovery
            if fabric_table and ('prev_ai_f_tab' not in st.session_state or st.session_state.prev_ai_f_tab != fabric_table):
                try:
                    from backend.fabric_connector import FabricConnector
                    creds = st.session_state.connector_creds
                    connector = FabricConnector(creds.get('fabric_tenant_id', ''), creds.get('fabric_client_id', ''), creds.get('fabric_client_secret', ''))
                    schema = connector.fetch_table_schema(f_sql, fabric_table, database_name=f_db, access_token=st.session_state.get('ai_f_token'))
                    if schema:
                        st.session_state.ai_discovered_cols = [c['name'] for c in schema]
                        st.session_state.prev_ai_f_tab = fabric_table
                except Exception: pass

            # API Failsafe inside the setup expander
            with st.expander("🔑 API Settings (Failsafe)"):
                direct_key = st.text_input("Direct API Key", value="", type="password", key="ai_direct_api_key")
                check_key = direct_key or os.getenv("AI_API_KEY")
                if check_key and len(check_key) > 8:
                    st.success(f"✅ Key Loaded: `{check_key[:4]}...{check_key[-4:]}`")

        # Business Requirement Input (Outside setup expander for visibility)
        requirement = st.text_area("Business Requirement / Context", 
                                  height=100, 
                                  placeholder="Example: We need to comply with GDPR...",
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
                    # Use the specific database provided by the user
                    token = st.session_state.get('ai_f_token')
                    schema = connector.fetch_table_schema(f_sql, fabric_table, database_name=f_db, access_token=token)
                    if schema:
                        cols_to_analyze = [c['name'] for c in schema]
                    else:
                        st.error(f"Could not fetch table schema for '{fabric_table}' in '{f_db or 'Fabric'}'. Discovery failed.")
                        return
                except Exception as e:
                    st.error(f"Fabric Analysis Error: {str(e)}")
                    return
        
        if not requirement and not cols_to_analyze:
            st.warning("Please provide context (requirement or schema) for analysis.")
        else:
            # Main Analysis Logic
            with st.spinner("Analyzing..."):
                suggestions = generate_cde_suggestions(requirement, industry=selected_industry, file_columns=cols_to_analyze, direct_key=direct_key)
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
                                "notes": f"Recommended by AI from {source_system}. Context: {requirement[:50]}..."
                            }
                            st.session_state.cdes.append(new_cde)
                            time.sleep(0.5)
                            st.rerun()
                st.divider()
