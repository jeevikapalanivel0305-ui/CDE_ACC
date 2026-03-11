"""
Microsoft Purview Connector with Enhanced Error Handling
- Authentication
- Catalog asset discovery (Datamap / Atlas)
- Critical Data Element (CDE) discovery (Data Governance)

Author: Jeevika
"""

import requests
import socket


class PurviewConnector:
    def __init__(self, account_name, tenant_id, client_id, client_secret):
        self.account_name = account_name.strip()
        self.tenant_id = tenant_id.strip()
        self.client_id = client_id.strip()
        self.client_secret = client_secret.strip()

        self.token = None
        self.base_url = f"https://{self.account_name}.purview.azure.com"
        self.hostname = f"{self.account_name}.purview.azure.com"

    # =========================================================
    # DNS AND NETWORK VALIDATION
    # =========================================================
    def validate_network(self):
        """Validate DNS resolution and network connectivity"""
        try:
            # Try to resolve the hostname
            socket.gethostbyname(self.hostname)
            return True, f"DNS resolution successful for {self.hostname}"
        except socket.gaierror as e:
            return False, f"DNS resolution failed for {self.hostname}. Error: {str(e)}"
        except Exception as e:
            return False, f"Network validation failed: {str(e)}"

    def validate_account_name(self):
        """Validate Purview account name format"""
        if not self.account_name:
            return False, "Purview account name is empty"
        
        # Account name should be lowercase alphanumeric and hyphens only
        if not self.account_name.replace('-', '').isalnum():
            return False, "Invalid account name format. Use only lowercase letters, numbers, and hyphens"
        
        if len(self.account_name) < 3 or len(self.account_name) > 63:
            return False, "Account name must be between 3 and 63 characters"
        
        return True, "Account name format is valid"

    # =========================================================
    # AUTHENTICATION
    # =========================================================
    def authenticate(self, debug=False):
        """Authenticate with Azure AD"""
        # First validate account name
        valid, msg = self.validate_account_name()
        if not valid:
            return False, f"Account validation failed: {msg}"
        
        # Then check network connectivity
        valid, msg = self.validate_network()
        if not valid:
            return False, f"Network validation failed: {msg}. Please check:\n1. Purview account name is correct\n2. You have internet connectivity\n3. DNS can resolve Azure domains\n4. VPN/Firewall is not blocking access"
        
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://purview.azure.net/.default"
        }

        if debug:
            print(f"Authenticating to Azure AD for tenant: {self.tenant_id}")
            print(f"Target Purview account: {self.account_name}")

        try:
            resp = requests.post(url, data=payload, timeout=30)

            if resp.status_code != 200:
                error_detail = resp.json().get('error_description', resp.text)
                if "AADSTS700016" in error_detail:
                    return False, f"Error: Application (Client ID) not found in this Tenant. Please check that you are using the correct Tenant ID and Client ID pair. \nDataset: {error_detail}"
                return False, f"Authentication failed (HTTP {resp.status_code}): {error_detail}"

            self.token = resp.json()["access_token"]

            if debug:
                print("✅ Authentication successful")

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
    # TEST CONNECTION
    # =========================================================
    def test_connection(self, debug=False):
        """Test connection to Purview with comprehensive validation"""
        stats = {}
        
        # Step 1: Validate account name
        valid, msg = self.validate_account_name()
        stats['account_validation'] = 'OK' if valid else f'FAILED: {msg}'
        if not valid:
            return False, msg, stats
        
        # Step 2: Validate network
        valid, msg = self.validate_network()
        stats['network_validation'] = 'OK' if valid else f'FAILED: {msg}'
        if not valid:
            return False, msg, stats
        
        # Step 3: Authenticate
        success, msg = self.authenticate(debug=debug)
        stats['authentication'] = 'OK' if success else f'FAILED: {msg}'
        if not success:
            return False, msg, stats

        # Step 4: Test Catalog API
        try:
            url = f"{self.base_url}/datamap/api/search/query"
            params = {"api-version": "2023-09-01"}
            payload = {"keywords": "*", "limit": 1}

            r = requests.post(
                url,
                headers=self._headers(),
                params=params,
                json=payload,
                timeout=30
            )

            if r.status_code == 200:
                stats["catalog_access"] = "OK"
                stats["sample_assets"] = len(r.json().get("value", []))
            else:
                stats["catalog_error"] = f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as e:
            stats["catalog_error"] = str(e)

        # Step 5: Test CDE API
        try:
            url = f"{self.base_url}/datagovernance/catalog/criticalDataElements"
            params = {"api-version": "2025-09-15-preview"}

            r = requests.get(
                url,
                headers=self._headers(),
                params=params,
                timeout=30
            )

            if r.status_code == 200:
                cde_data = r.json()
                stats["cde_count"] = len(cde_data.get("value", []))
                stats["cde_access"] = "OK"
            elif r.status_code == 404:
                stats["cde_error"] = "CDE API endpoint not found (404). This Purview instance may not have Data Governance enabled"
            else:
                stats["cde_error"] = f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as e:
            stats["cde_error"] = str(e)

        return True, "Connection test completed", stats

    # =========================================================
    # SEARCH CATALOG ASSETS
    # =========================================================
    def search_assets(self, limit=100, debug=False):
        """Search catalog assets"""
        success, msg = self.authenticate(debug=debug)
        if not success:
            raise Exception(msg)

        url = f"{self.base_url}/datamap/api/search/query"
        params = {"api-version": "2023-09-01"}
        payload = {"keywords": "*", "limit": limit}

        try:
            r = requests.post(
                url,
                headers=self._headers(),
                params=params,
                json=payload,
                timeout=30
            )

            if r.status_code != 200:
                raise Exception(f"Search failed (HTTP {r.status_code}): {r.text}")

            return r.json().get("value", [])
        
        except requests.exceptions.RequestException as e:
            raise Exception(f"Search request failed: {str(e)}")

    # =========================================================
    # GET ENTITY BY GUID
    # =========================================================
    def get_entity(self, guid, debug=False):
        """Get entity details by GUID"""
        success, msg = self.authenticate(debug=debug)
        if not success:
            raise Exception(msg)

        url = f"{self.base_url}/datamap/api/atlas/v2/entity/guid/{guid}"
        params = {"api-version": "2023-09-01"}

        try:
            r = requests.get(
                url,
                headers=self._headers(),
                params=params,
                timeout=30
            )

            return r.json() if r.status_code == 200 else None
        
        except requests.exceptions.RequestException as e:
            raise Exception(f"Get entity failed: {str(e)}")

    # =========================================================
    # FETCH CRITICAL DATA ELEMENTS
    # =========================================================
    def fetch_cdes(self, debug=False):
        """Fetch Critical Data Elements from Purview Data Governance"""
        success, msg = self.authenticate(debug=debug)
        if not success:
            raise Exception(f"Authentication failed: {msg}")

        url = f"{self.base_url}/datagovernance/catalog/criticalDataElements"
        params = {"api-version": "2025-09-15-preview"}

        if debug:
            print(f"Fetching CDEs from: {url}")
            print(f"Parameters: {params}")

        try:
            r = requests.get(
                url,
                headers=self._headers(),
                params=params,
                timeout=30
            )

            if debug:
                print(f"Response status: {r.status_code}")
                print(f"Response headers: {dict(r.headers)}")
                print(f"Response body (first 500 chars): {r.text[:500]}")

            if r.status_code == 404:
                raise Exception("CDE API endpoint not found (404). This Purview instance may not have Data Governance enabled or the API version may be incorrect")
            
            if r.status_code != 200:
                raise Exception(f"Failed to fetch CDEs (HTTP {r.status_code}): {r.text[:500]}")

            cdes = []
            response_data = r.json()
            
            if debug:
                print(f"Response data keys: {response_data.keys()}")
                print(f"Number of CDEs found: {len(response_data.get('value', []))}")
            
            for item in response_data.get("value", []):
                if isinstance(item, dict):
                    cdes.append(self._map_cde(item))
                else:
                    cdes.append({
                        "id": None,
                        "name": str(item),
                        "description": "",
                        "domain": "Reference",
                        "status": "Active",
                        "owner": None,
                        "steward": None,
                        "sourceSystem": "Purview"
                    })

            return cdes
        
        except requests.exceptions.ConnectionError as e:
            raise Exception(f"Connection error: {str(e)}. Please check:\n1. Purview account name is correct\n2. Network connectivity to Azure\n3. VPN/Firewall settings")
        except requests.exceptions.Timeout as e:
            raise Exception(f"Request timeout: {str(e)}. The Purview service may be slow or unavailable")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {str(e)}")

    # =========================================================
    # SAFE CDE MAPPING
    # =========================================================
    def _map_cde(self, cde):
        """Map Purview CDE to standard format"""
        domain = cde.get("domain")
    
        if isinstance(domain, dict):
            domain_name = domain.get("name", "Reference")
        elif isinstance(domain, str):
            # Check if it looks like a UUID (len 36, contains dashes)
            if len(domain) == 36 and '-' in domain:
                # Try to infer from name/description
                domain_name = self._infer_domain(cde.get("name"), cde.get("description"))
            else:
                domain_name = domain
        else:
            # Try to infer from name/description
            domain_name = self._infer_domain(cde.get("name"), cde.get("description"))
    
        return {
            "id": cde.get("id"),
            "name": cde.get("name", "Unnamed CDE"),
            "description": self._clean_html(cde.get("description", "")),
            "definition": self._clean_html(cde.get("description", "")),
            "domain": domain_name,
            "status": cde.get("status", "Active"),
            "owner": self._get_contact(cde, "owners"),
            "steward": self._get_contact(cde, "dataStewards"),
            "sourceSystem": "Purview",
            # Default risk scores
            "businessImpact": 3,
            "regulatoryCompliance": 3,
            "dataQualityRisk": 3,
            "securityRisk": 3,
            "systemComplexity": 3,
            "recoveryDifficulty": 3,
            "dataType": cde.get("dataType", ""),
            "downstreamSystems": "",
            "regulatory": "",
            "assessmentDate": "",
            "assessmentDate": "",
            "notes": "Imported from Microsoft Purview"
        }

    def _clean_html(self, text):
        """Remove HTML tags from text"""
        if not text:
            return ""
        import re
        clean = re.compile('<.*?>')
        return re.sub(clean, '', str(text))

    def _infer_domain(self, name, description):
        """Infer domain based on keywords in name and description"""
        text = (str(name) + " " + str(description)).lower()
        
        keywords = {
            'Healthcare': ['patient', 'doctor', 'hospital', 'medical', 'drug', 'treatment', 'diagnosis', 'clinical', 'provider', 'health'],
            'Finance / Banking': ['account', 'bank', 'credit', 'tax', 'transaction', 'payment', 'balance', 'loan', 'gl', 'ledger', 'financial', 'finance', 'wealth'],
            'Retail / E-Commerce': ['customer', 'product', 'order', 'sale', 'store', 'inventory', 'price', 'item', 'sku', 'market', 'shop', 'cart', 'merchant'],
            'Insurance': ['policy', 'claim', 'premium', 'coverage', 'underwriter', 'risk'],
            'Manufacturing': ['plant', 'factory', 'machine', 'production', 'assembly', 'supply', 'material'],
            'Energy / Utilities': ['grid', 'power', 'oil', 'gas', 'renewable', 'utility', 'energy', 'electric', 'water'],
            'Government': ['citizen', 'regulation', 'law', 'compliance', 'agency', 'gov', 'public'],
            'General': ['reference', 'master', 'dimension', 'lookup', 'code', 'common']
        }
        
        for domain, keys in keywords.items():
            for key in keys:
                if key in text:
                    return domain
                    
        return "General"

    def _get_contact(self, cde, role):
        """Safely extract contact information"""
        try:
            contacts = cde.get("contacts", {}).get(role, [])
            if isinstance(contacts, list) and contacts:
                contact = contacts[0]
                if isinstance(contact, dict):
                    return contact.get("displayName") or contact.get("name") or contact.get("email")
                return str(contact)
            return None
        except Exception:
            return None