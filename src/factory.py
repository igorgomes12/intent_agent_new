"""
Factory
=======
Factory para criar instâncias do IntentAgent com todas as dependências.
Usa Firestore como RAG de dados e API Gemini direta (Google AI Studio).
"""

from .config.settings import Settings
from .services.filter_extractor import FilterExtractor
from .strategies.local_strategy import LocalStrategy
from .strategies.ai_strategy import AIStrategy
from .agent.intent_agent import IntentAgent


class IntentAgentFactory:
    """Factory para criar IntentAgent"""
    
    @staticmethod
    def create(settings: Settings = None) -> IntentAgent:
        """
        Cria IntentAgent com todas as dependências
        
        Args:
            settings: configurações (se None, carrega do ambiente)
            
        Returns:
            IntentAgent configurado
        """
        if settings is None:
            settings = Settings.from_env()
        
        # Repository (local ou híbrido)
        repository = IntentAgentFactory._create_repository(settings)
        
        # Services
        filter_extractor = FilterExtractor(repository)
        
        # Decidir qual serviço de AI usar
        import os
        use_vertex = os.getenv('USE_VERTEX_AI', 'false').lower() == 'true'
        
        if use_vertex:
            # Tentar Vertex AI (sem limite de quota, requer billing)
            try:
                from .services.ai_inference_vertex import AIInferenceServiceVertex
                ai_service = AIInferenceServiceVertex(
                    project_id=settings.gcp_project_id,
                    location=settings.gcp_location,
                    credentials_path=settings.gcp_credentials_path
                )
            except Exception:
                # Fallback silencioso para Google AI Studio
                api_key = os.getenv('GOOGLE_API_KEY')
                if not api_key:
                    raise RuntimeError("❌ Vertex AI indisponível e GOOGLE_API_KEY não encontrada")
                print("✅ Usando Google AI Studio (limite 20/dia)")
                from .services.ai_inference_gemini import AIInferenceServiceGemini
                ai_service = AIInferenceServiceGemini(api_key=api_key)
        else:
            # Usar Google AI Studio (tier gratuito, limite 20/dia)
            api_key = os.getenv('GOOGLE_API_KEY')
            if not api_key:
                print("⚠️  GOOGLE_API_KEY não encontrada no .env")
                print("   Tentando usar Vertex AI como fallback...")
                from .services.ai_inference import AIInferenceService
                ai_service = AIInferenceService(
                    project_id=settings.gcp_project_id,
                    location=settings.gcp_location,
                    credentials_path=settings.gcp_credentials_path
                )
            else:
                print("✅ Usando Google AI Studio (limite 20/dia)")
                from .services.ai_inference_gemini import AIInferenceServiceGemini
                ai_service = AIInferenceServiceGemini(api_key=api_key)
        
        # Dictionary service (mappers locais de valores)
        from .dictionaries.dictionary_service import DictionaryService
        dictionary_service = DictionaryService()

        # Strategies
        local_strategy = LocalStrategy(filter_extractor)
        ai_strategy = AIStrategy(ai_service)
        ai_strategy._repo = repository
        ai_strategy._dictionary_service = dictionary_service
        
        # Agent (modo novo - sem matchers)
        return IntentAgent(
            repository=repository,
            local_strategy=local_strategy,
            ai_strategy=ai_strategy,
            gemini_threshold=settings.gemini_threshold
        )
    
    @staticmethod
    def _create_repository(settings: Settings):
        """Cria repositório baseado nas configurações"""

        import os
        use_local = os.getenv('USE_LOCAL_REPO', 'false').lower() == 'true'

        if use_local:
            from .repositories.local_json_repository import LocalJsonRepository
            print("📂 Usando repositório local (JSON)")
            return LocalJsonRepository()

        if not settings.use_firestore:
            raise ValueError("❌ Firestore está desabilitado. Configure USE_FIRESTORE=true ou USE_LOCAL_REPO=true no .env")

        # Modo Firestore
        from .repositories.firestore_firebase_repository import FirestoreFirebaseRepository

        firebase_repo = FirestoreFirebaseRepository(
            project_id=settings.firestore_project_id or settings.gcp_project_id,
            database=settings.firestore_database,
            credentials_path=settings.firestore_credentials_path
        )

        print("🔥 Usando Firestore como RAG de dados")
        return firebase_repo
