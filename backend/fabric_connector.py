"""
Microsoft Fabric Connector
- Authentication via Azure AD
- Fabric Item discovery (Workspaces -> Items)
- Maps Fabric Items to CDEs

Author: Jeevika
"""

import requests
import socket
import json
import pyodbc
import pandas as pd

class FabricConnector:
    def __init__(self, tenant_id, client_id, client_secret):
        self.tenant_id = tenant_id.strip()
        self.client_id = client_id.strip()
        self.client_secret = client_secret.strip()
        self.token = None
        self.base_url = "https://api.fabric.microsoft.com/v1"

    # =========================================================
    # AUTHENTICATION
    # =========================================================
    def authenticate(self, debug=False):
        """Authenticate with Azure AD for Fabric"""
        try:
            url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
            
            # Fabric scope
            scope = "https://api.fabric.microsoft.com/.default"
            
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": scope
            }

            if debug:
                print(f"Authenticating to Azure AD for tenant: {self.tenant_id}")

            resp = requests.post(url, data=payload, timeout=30)

            if resp.status_code != 200:
                error_detail = resp.json().get('error_description', resp.text)
                if "AADSTS700016" in error_detail:
                    return False, f"Error: Application (Client ID) not found in this Tenant. Please check that you are using the correct Tenant ID and Client ID pair. \nDataset: {error_detail}"
                return False, f"Authentication failed (HTTP {resp.status_code}): {error_detail}"

            self.token = resp.json().get("access_token")
            
            if not self.token:
                 return False, "Authentication failed: No access token received"

            if debug:
                print(" Authentication successful")

            return True, "Authenticated successfully"
        
        except requests.exceptions.RequestException as e:
            return False, f"Authentication request failed: {str(e)}"
        except Exception as e:
            return False, f"Unexpected authentication error: {str(e)}"

    def _headers(self):
        """Get authorization headers"""
        if not self.token:
            raise Exception("Not authenticated. Call authenticate() first")
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    # =========================================================
    # WORKSPACE & ITEM DISCOVERY (REST API – no port 1433)
    # =========================================================
    def list_workspaces(self):
        """List all Fabric workspaces accessible to the service principal."""
        url = f"{self.base_url}/workspaces"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        if resp.status_code == 200:
            return resp.json().get('value', [])
        raise Exception(
            f"Failed to list workspaces (HTTP {resp.status_code}): "
            f"{resp.json().get('message', resp.text)}"
        )

    def list_workspace_items(self, workspace_id, item_type=None):
        """List items inside a workspace. Optionally filter by item_type (e.g. 'Lakehouse', 'Warehouse')."""
        url = f"{self.base_url}/workspaces/{workspace_id}/items"
        params = {"type": item_type} if item_type else {}
        resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json().get('value', [])
        raise Exception(
            f"Failed to list workspace items (HTTP {resp.status_code}): "
            f"{resp.json().get('message', resp.text)}"
        )

    def list_lakehouse_tables(self, workspace_id, lakehouse_id):
        """List tables in a Fabric Lakehouse via the Lakehouse Tables REST API (no SQL required)."""
        url = f"{self.base_url}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        if resp.status_code == 200:
            body = resp.json()
            # API returns either {"data": [...]} or {"value": [...]}
            return body.get('data', body.get('value', []))
        raise Exception(
            f"Failed to list lakehouse tables (HTTP {resp.status_code}): "
            f"{resp.json().get('message', resp.text)}"
        )

    def list_warehouses(self, workspace_id):
        """List Fabric Warehouses in a workspace via REST API."""
        url = f"{self.base_url}/workspaces/{workspace_id}/warehouses"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        if resp.status_code == 200:
            return resp.json().get('value', [])
        raise Exception(
            f"Failed to list warehouses (HTTP {resp.status_code}): "
            f"{resp.json().get('message', resp.text)}"
        )

    def _get_token_for_scope(self, scope):
        """Get a fresh access token for a given OAuth2 scope (used for multi-strategy auth)."""
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        resp = requests.post(url, data={
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": scope,
        }, timeout=30)
        if resp.status_code == 200:
            return resp.json().get("access_token")
        raise Exception(
            f"Token error ({resp.status_code}): "
            f"{resp.json().get('error_description', resp.text)}"
        )

    def list_warehouse_tables_rest(self, workspace_id, warehouse_id, warehouse_name=None):
        """
        List tables in a Fabric Warehouse using REST APIs — no SQL / port 1433.

        Tries three strategies in order:
          1. Fabric executeQuery API  (Fabric scope)
          2. Fabric executeQuery API  (Power BI scope)
          3. Power BI executeQueries  (DAX INFO.TABLES on the warehouse semantic model)
        Raises a descriptive exception if all three fail.
        """
        errors = []

        # ── Strategy 1: Fabric executeQuery (Fabric scope) ──────────────────
        try:
            url = (f"{self.base_url}/workspaces/{workspace_id}"
                   f"/warehouses/{warehouse_id}/executeQuery")
            payload = {
                "queryText": (
                    "SELECT TABLE_SCHEMA, TABLE_NAME "
                    "FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_TYPE = 'BASE TABLE' "
                    "ORDER BY TABLE_SCHEMA, TABLE_NAME"
                )
            }
            resp = requests.post(url, headers=self._headers(), json=payload, timeout=60)
            if resp.status_code in (200, 202):
                data = resp.json()
                results = data.get('results', data.get('data', []))
                if isinstance(results, list) and results:
                    rows = results[0].get('rows', [])
                    if rows:
                        return [
                            f"{row[0]}.{row[1]}" if row[0] not in ('dbo', '') else row[1]
                            for row in rows if len(row) >= 2
                        ]
            errors.append(
                f"Strategy 1 (Fabric executeQuery): "
                f"HTTP {resp.status_code} — {resp.text[:300]}"
            )
        except Exception as exc:
            errors.append(f"Strategy 1 (Fabric executeQuery): {exc}")

        # ── Strategy 2: Fabric executeQuery (Power BI scope) ────────────────
        try:
            pbi_scope = "https://analysis.windows.net/powerbi/api/.default"
            pbi_token = self._get_token_for_scope(pbi_scope)
            pbi_headers = {
                "Authorization": f"Bearer {pbi_token}",
                "Content-Type": "application/json",
            }
            url = (f"{self.base_url}/workspaces/{workspace_id}"
                   f"/warehouses/{warehouse_id}/executeQuery")
            resp = requests.post(url, headers=pbi_headers, json=payload, timeout=60)
            if resp.status_code in (200, 202):
                data = resp.json()
                results = data.get('results', data.get('data', []))
                if isinstance(results, list) and results:
                    rows = results[0].get('rows', [])
                    if rows:
                        return [
                            f"{row[0]}.{row[1]}" if row[0] not in ('dbo', '') else row[1]
                            for row in rows if len(row) >= 2
                        ]
            errors.append(
                f"Strategy 2 (Fabric executeQuery / PBI token): "
                f"HTTP {resp.status_code} — {resp.text[:300]}"
            )
        except Exception as exc:
            errors.append(f"Strategy 2 (Fabric executeQuery / PBI token): {exc}")

        # ── Strategy 3: Power BI executeQueries – DAX INFO.TABLES() ─────────
        try:
            pbi_scope = "https://analysis.windows.net/powerbi/api/.default"
            pbi_token = self._get_token_for_scope(pbi_scope)
            pbi_headers = {
                "Authorization": f"Bearer {pbi_token}",
                "Content-Type": "application/json",
            }

            # Find the semantic model linked to this warehouse in the workspace
            ds_resp = requests.get(
                f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets",
                headers=pbi_headers, timeout=30
            )
            if ds_resp.status_code != 200:
                raise Exception(
                    f"Datasets list HTTP {ds_resp.status_code}: {ds_resp.text[:200]}"
                )

            datasets = ds_resp.json().get('value', [])
            # Prefer the dataset with the same name as the warehouse; else try all
            if warehouse_name:
                ordered = sorted(
                    datasets,
                    key=lambda d: 0 if d.get('name', '').lower() == warehouse_name.lower() else 1
                )
            else:
                ordered = datasets

            for ds in ordered:
                dataset_id = ds['id']
                dax_resp = requests.post(
                    f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
                    f"/datasets/{dataset_id}/executeQueries",
                    headers=pbi_headers,
                    json={
                        "queries": [{
                            "query": (
                                "EVALUATE SELECTCOLUMNS("
                                "  FILTER(INFO.TABLES(), NOT [IsHidden]),"
                                "  \"Name\", [Name]"
                                ")"
                            )
                        }],
                        "serializerSettings": {"includeNulls": True},
                    },
                    timeout=60,
                )
                if dax_resp.status_code == 200:
                    dax_data = dax_resp.json()
                    rows = (
                        dax_data.get('results', [{}])[0]
                        .get('tables', [{}])[0]
                        .get('rows', [])
                    )
                    # Column key may be "Name" or "[Name]" depending on API version
                    tables = [
                        row.get('Name', row.get('[Name]', ''))
                        for row in rows
                    ]
                    tables = [t for t in tables if t]
                    if tables:
                        return tables
            errors.append(
                "Strategy 3 (PBI DAX INFO.TABLES): no tables returned from any dataset"
            )
        except Exception as exc:
            errors.append(f"Strategy 3 (PBI DAX INFO.TABLES): {exc}")

        # ── All strategies failed ────────────────────────────────────────────
        raise Exception(
            "Could not list tables from the Fabric Warehouse.\n\n"
            "Troubleshooting checklist:\n"
            "• Service Principal must have the 'Contributor' role on the Fabric workspace\n"
            "• The warehouse must contain at least one table\n"
            "• For DAX strategy: the SP needs 'Dataset.ReadWrite.All' in Power BI\n\n"
            "Technical details:\n" + "\n".join(errors)
        )

    @staticmethod
    def parse_guids_from_connection_string(connection_string):
        """Extract all GUIDs from a Fabric SQL connection string.

        Fabric Warehouse SQL endpoints typically look like:
          <workspaceId>.datawarehouse.fabric.microsoft.com
        or
          <warehouseId>-<workspaceId>.datawarehouse.fabric.microsoft.com
        Returns a list of all GUIDs found (order: first found = potential workspace_id).
        """
        import re as _re
        return _re.findall(
            r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
            connection_string.lower()
        )

    # =========================================================
    # FETCH FABRIC ITEMS (Real REST API)
    # =========================================================
    def fetch_cdes(self, debug=False):
        """
        Fetch CDEs from Fabric by listing all workspace items via the REST API.
        Maps Lakehouses, Warehouses, and Semantic Models to CDEs.
        Falls back to sample data only if the API call fails.
        """
        if not self.token:
            success, msg = self.authenticate(debug)
            if not success:
                raise Exception(msg)

        try:
            if debug:
                print("[Fabric] Fetching workspaces via REST API...")

            workspaces = self.list_workspaces()

            fabric_items = []
            for ws in workspaces:
                ws_id = ws.get('id')
                ws_name = ws.get('displayName', 'Unknown Workspace')
                try:
                    items = self.list_workspace_items(ws_id)
                    for item in items:
                        item['workspaceId'] = ws_id
                        item['workspaceName'] = ws_name
                        fabric_items.append(item)
                except Exception as item_err:
                    if debug:
                        print(f"[Fabric] Could not fetch items for workspace '{ws_name}': {item_err}")

            if debug:
                print(f"[Fabric] Found {len(fabric_items)} items across {len(workspaces)} workspaces")

            return [self._map_to_cde(item) for item in fabric_items]

        except Exception as e:
            raise Exception(f"Failed to fetch Fabric items: {str(e)}")

    def _clean_html(self, text):
        """Remove HTML tags from text"""
        if not text:
            return ""
        import re
        clean = re.compile('<.*?>')
        return re.sub(clean, '', str(text))

    def _map_to_cde(self, item):
        """Map Fabric Item to CDE schema"""
        # Determine domain from workspace name if possible
        domain = "Reference"
        
        # Combine workspace name and item name for better context
        text = (str(item.get('workspaceName', '')) + " " + str(item.get('displayName', ''))).lower()
        
        keywords = {
            'Healthcare': ['patient', 'doctor', 'hospital', 'medical', 'drug', 'treatment', 'diagnosis', 'clinical', 'provider', 'health'],
            'Finance / Banking': ['account', 'bank', 'credit', 'tax', 'transaction', 'payment', 'balance', 'loan', 'gl', 'ledger', 'financial', 'finance', 'wealth'],
            'Retail / E-Commerce': ['customer', 'product', 'order', 'sale', 'store', 'inventory', 'price', 'item', 'sku', 'market', 'shop', 'cart', 'merchant'],
            'Insurance': ['policy', 'claim', 'premium', 'coverage', 'underwriter', 'risk'],
            'Manufacturing': ['plant', 'factory', 'machine', 'production', 'assembly', 'supply', 'material', 'ops', 'operations'],
            'Energy / Utilities': ['grid', 'power', 'oil', 'gas', 'renewable', 'utility', 'energy', 'electric', 'water'],
            'Government': ['citizen', 'regulation', 'law', 'compliance', 'agency', 'gov', 'public'],
            'General': ['reference', 'master', 'dimension', 'lookup', 'code', 'common', 'shared']
        }
        
        domain_found = False
        for d, keys in keywords.items():
            for key in keys:
                if key in text:
                    domain = d
                    domain_found = True
                    break
            if domain_found:
                break
        
        return {
            "id": None, # Will be assigned on import
            "name": item.get("displayName", "Unnamed Fabric Item"),
            "description": self._clean_html(item.get("description", "")),
            "definition": self._clean_html(item.get("description", "")),
            "domain": domain,
            "status": "Active",
            "owner": "Fabric Admin", # Placeholder
            "steward": "Workspace Admin", # Placeholder
            "sourceSystem": "Microsoft Fabric",
            "dataType": item.get("type", "Unknown"), # e.g. Lakehouse, Warehouse
            # Default risk scores
            "businessImpact": 3,
            "regulatoryCompliance": 3,
            "dataQualityRisk": 3,
            "securityRisk": 3,
            "systemComplexity": 3,
            "recoveryDifficulty": 3,
            "downstreamSystems": "",
            "regulatory": "",
            "assessmentDate": "",
            "notes": f"Imported from Fabric Workspace: {item.get('workspaceName')}"
        }

    # =========================================================
    # SQL ENDPOINT INTEGRATION (Hybrid Mode)
    # =========================================================
    def get_sql_connection(self, raw_endpoint, database_name=None, access_token=None):
        """
        Creates a pyodbc connection to the Fabric SQL Connection string.
        Supports Service Principal, Interactive, and Token-based (MSAL) auth.
        """
        import pyodbc
        import struct
        
        # SQL_COPT_SS_ACCESS_TOKEN is 1256 (for pyodbc)
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        
        try:
            # Clean the string
            raw_endpoint = str(raw_endpoint).strip()
            if raw_endpoint.startswith("https://"): raw_endpoint = raw_endpoint.replace("https://", "")
            if raw_endpoint.startswith("tcp:"): raw_endpoint = raw_endpoint.replace("tcp:", "")
            
            # 0. Validate if it's an API URL instead of a SQL Endpoint
            if "api.fabric.microsoft.com" in raw_endpoint.lower():
                raise Exception("The URL provided looks like a Fabric API URL. Please use the 'SQL Connection String' (e.g. xxxxxxx.datawarehouse.fabric.microsoft.com)")

            # 1. Identify valid driver
            drivers = pyodbc.drivers()
            best_driver = next((d for d in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"] if d in drivers), "SQL Server")
            
            # 2. Build base connection string
            server_name = raw_endpoint.split(";")[0]
            if "," not in server_name and ":" not in server_name:
                server_name += ",1433"
            
            connection_string = f"Driver={{{best_driver}}};Server={server_name}"
            connection_string += ";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=90"
            
            if database_name:
                connection_string += f";Database={database_name}"
            elif "DATABASE=" in raw_endpoint.upper():
                 db_part = raw_endpoint.upper().split("DATABASE=")[1].split(";")[0]
                 connection_string += f";Database={db_part}"

            # 3. Handle Authentication
            if access_token:
                # TOKEN MODE: String must NOT have UID/PWD/Authentication
                print(" [SQL] Connecting using Access Token (MSAL)...")
            elif "AUTHENTICATION=" not in connection_string.upper():
                if self.client_id and self.client_secret:
                    # Service Principal
                    connection_string += f";UID={self.client_id};PWD={self.client_secret};Authentication=ActiveDirectoryServicePrincipal"
                elif self.client_secret and self.client_secret.startswith("AAD_PWD:"):
                    # AAD Password
                    actual_pwd = self.client_secret.replace("AAD_PWD:", "")
                    connection_string += f";UID={self.client_id};PWD={actual_pwd};Authentication=ActiveDirectoryPassword"
                elif self.client_id:
                    # Interactive with hint
                    connection_string += f";UID={self.client_id};Authentication=ActiveDirectoryInteractive"
                else:
                    # Base Interactive
                    connection_string += ";Authentication=ActiveDirectoryInteractive"

            # Mask logging
            log_str = connection_string
            if "PWD=" in log_str:
                import re
                log_str = re.sub(r"PWD=[^;]+", "PWD=********", connection_string)
            print(f" [SQL] String: {log_str}")
            
            # 4. Connect
            if access_token:
                # Convert token to bytes for ODBC attribute (correct format for ODBC Driver on Linux/Windows)
                token_bytes = access_token.encode("utf-16-le")
                token_struct = struct.pack('<I', len(token_bytes)) + token_bytes
                conn = pyodbc.connect(connection_string, timeout=90, autocommit=True, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
            else:
                conn = pyodbc.connect(connection_string, timeout=90, autocommit=True)
                
            print(" [SQL] Connection successful.")
            return conn
        except Exception as e:
            print(f" [SQL] Connection error: {str(e)}")
            raise e

    def list_tables(self, connection_string, database_name=None, access_token=None):
        """List all user tables in the Fabric SQL Endpoint"""
        conn = None
        try:
            conn = self.get_sql_connection(connection_string, database_name, access_token=access_token)
            cursor = conn.cursor()
            # Query for user tables
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            tables = [row[0] for row in cursor.fetchall()]
            return tables
        except Exception as e:
            raise Exception(f"Failed to list tables: {str(e)}")
        finally:
            if conn: conn.close()

    def fetch_table_schema(self, connection_string, table_name, database_name=None, access_token=None):
        """Fetch column names and types from a Fabric table"""
        conn = None
        try:
            conn = self.get_sql_connection(connection_string, database_name, access_token=access_token)
            cursor = conn.cursor()
            
            # Extract schema and table name if provided as schema.table
            target_schema = 'dbo'
            target_table = table_name
            if '.' in table_name:
                parts = table_name.split('.')
                target_schema = parts[0]
                target_table = parts[1]

            # Use INFORMATION_SCHEMA for portability
            query = f"""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{target_table}'
            AND TABLE_SCHEMA = '{target_schema}'
            """
            cursor.execute(query)
            columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
            
            if not columns:
                # Fallback: try just table name if schema match fails
                cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
                columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
                
            if not columns:
                raise Exception(f"Table '{table_name}' not found or has no columns in database '{database_name or 'default'}'.")
                
            return columns
        finally:
            if conn:
                conn.close()

    def sync_to_fabric(self, df, connection_string, table_name, database_name=None, create_if_not_exists=True):
        """Append CDE Register data to a Fabric table, with optimized batch insertion"""
        conn = None
        try:
            # Prepare data (select relevant columns)
            cols = ['id', 'name', 'domain', 'definition', 'sourceSystem', 'businessImpact', 'regulatoryCompliance', 'dataQualityRisk']
            for col in cols:
                if col not in df.columns: df[col] = ""
            
            # Clean and format data for SQL
            df_sync = df[cols].copy()
            for col in ['businessImpact', 'regulatoryCompliance', 'dataQualityRisk']:
                df_sync[col] = pd.to_numeric(df_sync[col], errors='coerce').fillna(3).astype(int)
            df_sync = df_sync.fillna("")
            
            # Ensure schema prefix if missing
            if "." not in table_name:
                table_name = f"dbo.{table_name}"
            
            conn = self.get_sql_connection(connection_string, database_name)
            cursor = conn.cursor()
            
            # Step 1: Create table if needed
            if create_if_not_exists:
                print(f" [SQL] Preparing table '{table_name}'...")
                cursor.execute(f"""
                    IF OBJECT_ID('{table_name}', 'U') IS NULL 
                    CREATE TABLE {table_name} (
                        id VARCHAR(50), 
                        name VARCHAR(255), 
                        domain VARCHAR(100), 
                        definition VARCHAR(8000), 
                        sourceSystem VARCHAR(100), 
                        businessImpact INT, 
                        regulatoryCompliance INT, 
                        dataQualityRisk INT
                    )
                """)
                conn.commit()
            
            # Step 2: Batch Insert
            data_to_insert = [tuple(x) for x in df_sync.values]
            query = f"INSERT INTO {table_name} (id, name, domain, definition, sourceSystem, businessImpact, regulatoryCompliance, dataQualityRisk) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            
            print(f" [SQL] Syncing {len(data_to_insert)} records to {table_name}...")
            
            # Performance optimization: enable fast_executemany
            cursor.fast_executemany = True
            cursor.executemany(query, data_to_insert)
            
            conn.commit()
            print(f" [SQL] Sync complete for {table_name}.")
            return True, f"Successfully synced {len(data_to_insert)} records to '{table_name}'."
        except Exception as e:
            print(f" [SQL] Sync error: {str(e)}")
            if conn: conn.rollback()
            return False, f"Sync failed: {str(e)}"
        finally:
            if conn: conn.close()
