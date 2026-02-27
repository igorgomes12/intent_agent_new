"""
Settings
========
Configurações da aplicação.
"""

from dataclasses import dataclass


@dataclass
class Settings:
    """Configurações do agente"""
    
    # Vertex AI
    gcp_project_id: str
    gcp_location: str = "us-central1"
    gcp_credentials_path: str = None  # Opcional: caminho para key.json
    
    # Firestore (pode ser projeto diferente do Vertex AI)
    firestore_project_id: str = None  # Se None, usa gcp_project_id
    firestore_credentials_path: str = None  # Se None, usa gcp_credentials_path
    firestore_database: str = "(default)"
    use_firestore: bool = False
    
    # Paths
    flows_base_path: str = "./flows"
    
    # Thresholds
    gemini_threshold: float = 0.5
    
    # Vector Search
    use_vector_search: bool = False
    vector_search_index_endpoint: str = None
    vector_search_deployed_index_flows: str = None
    vector_search_deployed_index_tables: str = None
    
    # Document AI
    document_ai_processor_id: str = None
    
    # Cloud Spanner
    use_spanner: bool = False
    spanner_instance: str = None
    spanner_database: str = None
    
    @classmethod
    def from_env(cls):
        """Carrega configurações de variáveis de ambiente"""
        import os
        from dotenv import load_dotenv
        
        # Carregar .env
        load_dotenv()
        
        # Projeto e credenciais Vertex AI
        gcp_project = os.getenv("GCP_PROJECT_ID", "seu-projeto-gcp")
        gcp_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        
        # Firestore: se não especificado, usa mesmo projeto/credenciais do Vertex
        firestore_project = os.getenv("FIRESTORE_PROJECT_ID")
        if not firestore_project:
            firestore_project = gcp_project
        
        firestore_creds = os.getenv("FIRESTORE_CREDENTIALS")
        if not firestore_creds:
            firestore_creds = gcp_creds
        
        return cls(
            # Vertex AI
            gcp_project_id=gcp_project,
            gcp_location=os.getenv("GCP_LOCATION", "us-central1"),
            gcp_credentials_path=gcp_creds,
            
            # Firestore
            firestore_project_id=firestore_project,
            firestore_credentials_path=firestore_creds,
            firestore_database=os.getenv("FIRESTORE_DATABASE", "(default)"),
            use_firestore=os.getenv("USE_FIRESTORE", "false").lower() == "true",
            
            # Paths
            flows_base_path=os.getenv("FLOWS_PATH", "./flows"),
            
            # Thresholds
            gemini_threshold=float(os.getenv("GEMINI_THRESHOLD", "0.5")),
            
            # Vector Search
            use_vector_search=os.getenv("USE_VECTOR_SEARCH", "false").lower() == "true",
            vector_search_index_endpoint=os.getenv("VECTOR_SEARCH_INDEX_ENDPOINT"),
            vector_search_deployed_index_flows=os.getenv("VECTOR_SEARCH_DEPLOYED_INDEX_FLOWS"),
            vector_search_deployed_index_tables=os.getenv("VECTOR_SEARCH_DEPLOYED_INDEX_TABLES"),
            
            # Document AI
            document_ai_processor_id=os.getenv("DOCUMENT_AI_PROCESSOR_ID"),
            
            # Cloud Spanner
            use_spanner=os.getenv("USE_SPANNER", "false").lower() == "true",
            spanner_instance=os.getenv("SPANNER_INSTANCE"),
            spanner_database=os.getenv("SPANNER_DATABASE")
        )
