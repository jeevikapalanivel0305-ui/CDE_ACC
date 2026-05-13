import streamlit as st
import json
import time
import pandas as pd
from openai import AzureOpenAI

def get_ai_client():
    """Initialize Azure OpenAI client"""
    try:
        api_key = st.secrets.get("AZURE_OPENAI_API_KEY")
        endpoint = st.secrets.get("AZURE_OPENAI_ENDPOINT")
        api_version = st.secrets.get("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
        if not api_key or not endpoint:
            st.error(" AZURE_OPENAI_API_KEY or AZURE_OPENAI_ENDPOINT not found in secrets.")
            return None
        return AzureOpenAI(api_key=api_key, azure_endpoint=endpoint, api_version=api_version)
    except Exception as e:
        st.error(f"Error initializing Azure OpenAI client: {str(e)}")
        return None

# ============================================
# AI RECOMMENDATION LOGIC
# ============================================

def generate_cde_suggestions(business_requirement, industry="General", file_columns=None):
    """Generate CDE suggestions using Azure OpenAI based on business requirement, industry, and optional file schema"""
    client = get_ai_client()
    
    if not client:
        st.warning(" Azure OpenAI not configured. Please check your secrets.toml settings.")
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
        deployment = st.secrets.get("AZURE_OPENAI_DEPLOYMENTNAME", "gpt-4.1")
        max_tokens = int(st.secrets.get("MAX_TOKENS", 16384))
        response = client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens
        )
        
        response_text = response.choices[0].message.content
        # Clean up code blocks if present
        if "```json" in response_text:
            response_text = response_text.split("```json")[1].split("```")[0].strip()
        elif "```" in response_text:
            response_text = response_text.split("```")[1].split("```")[0].strip()
            
        result = json.loads(response_text)
        return result
    except Exception as e:
        st.error(f" Error generating AI suggestions: {str(e)}")
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
        deployment = st.secrets.get("AZURE_OPENAI_DEPLOYMENTNAME", "gpt-4.1")
        max_tokens = int(st.secrets.get("MAX_TOKENS", 16384))
        response = client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens
        )
        text = response.choices[0].message.content
        if "```json" in text: text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text: text = text.split("```")[1].split("```")[0].strip()
        return json.loads(text)
    except Exception as e:
        st.error(f" AI Error: {str(e)}")
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
    
    if "ai_state" not in st.session_state:
        st.session_state.ai_state = {
            "industry": "General",
            "connector": "Excel",
            "f_sql": "",
            "f_tab_sel": "--- Select Table ---",
            "f_tab_text": "",
            "requirement": ""
        }

    def sync_ai_industry():
        st.session_state.ai_state["industry"] = st.session_state.ai_selected_industry
    def sync_ai_connector():
        st.session_state.ai_state["connector"] = st.session_state.ai_connector_type
    def sync_ai_f_sql():
        st.session_state.ai_state["f_sql"] = st.session_state.ai_f_sql_input
    def sync_ai_f_tab_sel():
        st.session_state.ai_state["f_tab_sel"] = st.session_state.ai_f_tab_sel_ref
    def sync_ai_f_tab_text():
        st.session_state.ai_state["f_tab_text"] = st.session_state.ai_f_tab_text_ref
    def sync_ai_requirement():
        st.session_state.ai_state["requirement"] = st.session_state.ai_requirement
    
    col_ind, col_conn = st.columns(2)
    
    with col_ind:
        st.markdown("**1. Industry Domain**")
        ind_options = ["General", "Finance / Banking", "Healthcare", "Retail / E-Commerce", "Manufacturing", "Energy / Utilities", "Insurance"]
        ind_idx = ind_options.index(st.session_state.ai_state["industry"]) if st.session_state.ai_state["industry"] in ind_options else 0
        selected_industry = st.selectbox("Industry", ind_options, index=ind_idx, key="ai_selected_industry", on_change=sync_ai_industry)
        
    with col_conn:
        st.markdown("**2. Data Source**")
        conn_options = ["Excel", "Microsoft Fabric"]
        conn_idx = conn_options.index(st.session_state.ai_state["connector"]) if st.session_state.ai_state["connector"] in conn_options else 0
        connector_type = st.selectbox("Connector", conn_options, index=conn_idx, key="ai_connector_type", on_change=sync_ai_connector)
        
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
        # Fabric Connector UI
        creds = st.session_state.connector_creds
        f_sql_val = st.session_state.ai_state["f_sql"] if st.session_state.ai_state["f_sql"] else creds.get('fabric_sql_endpoint', '')

        st.info("Note: Direct SQL connection (port 1433) is blocked on Streamlit Cloud. Enter your table name and columns manually below, or use the Excel connector to upload your schema.")

        col_sql, col_db = st.columns([3, 1])
        with col_sql:
            f_sql = st.text_input("SQL Endpoint (for reference)",
                                 value=f_sql_val,
                                 type="password",
                                 key="ai_f_sql_input",
                                 on_change=sync_ai_f_sql)
        with col_db:
            f_db = st.text_input("Database / Warehouse",
                                value=creds.get('fabric_database', ''),
                                placeholder="e.g. w1",
                                key="ai_f_db_input")
            creds['fabric_database'] = f_db

        fabric_table = st.text_input("Table Name", placeholder="e.g. dbo.Sales_Transactions",
                                     value=st.session_state.ai_state["f_tab_text"],
                                     key="ai_f_tab_text_ref", on_change=sync_ai_f_tab_text)

        # Manual column entry
        manual_cols_input = st.text_area(
            "Column Names (comma-separated)",
            placeholder="e.g. CustomerID, OrderDate, TotalAmount, Region, ProductCode",
            height=80,
            key="ai_manual_cols"
        )
        if manual_cols_input.strip():
            manual_cols = [c.strip() for c in manual_cols_input.split(",") if c.strip()]
            st.session_state.ai_discovered_cols = manual_cols
            st.success(f"Using {len(manual_cols)} columns for analysis.")
        
        # Dummy vars so the rest of the code doesn't break
        fabric_tenant_id = creds.get('fabric_tenant_id', '')
        fabric_client_id = creds.get('fabric_client_id', '')
        fabric_client_secret = creds.get('fabric_client_secret', '')
        get_ep_token = None

    # Business Requirement Input
    requirement = st.text_area("Business Requirement / Context", 
                              height=100, 
                              placeholder="Example: We need to comply with GDPR for our European customer data...",
                              value=st.session_state.ai_state["requirement"],
                              key="ai_requirement", 
                              on_change=sync_ai_requirement)
    
    if st.button("Analyze & Recommend CDEs", type="primary"):
        cols_to_analyze = file_columns if connector_type == "Excel" else st.session_state.get('ai_discovered_cols', [])
        
        # For Fabric, use manually entered columns (SQL not reachable on Streamlit Cloud)
        if connector_type == "Microsoft Fabric":
            if not fabric_table and not cols_to_analyze:
                st.error("Please enter a Table Name and/or Column Names.")
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
                        st.button(" Added", key=f"added_btn_{i}", disabled=True)
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
