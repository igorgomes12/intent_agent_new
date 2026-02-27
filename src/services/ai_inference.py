"""
AI Inference Service
====================
Responsável pela inferência usando Vertex AI Gemini.
"""

import json
from typing import Dict
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig


class AIInferenceService:
    """Serviço de inferência com Vertex AI"""
    
    def __init__(self, project_id: str, location: str = "us-central1", 
                 credentials_path: str = None):
        """
        Args:
            project_id: ID do projeto GCP
            location: região (us-central1, etc)
            credentials_path: caminho para arquivo JSON de credenciais (opcional)
        """
        # Se forneceu credenciais e arquivo existe, configurar
        if credentials_path:
            import os
            if os.path.exists(credentials_path):
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
            else:
                print(f"⚠️  Credenciais não encontradas: {credentials_path}")
                print("   Usando credenciais padrão do sistema")
        
        vertexai.init(project=project_id, location=location)
        
        # Tentar diferentes modelos em ordem de preferência
        models_to_try = [
            "gemini-1.5-flash-002",
            "gemini-1.5-flash-001", 
            "gemini-1.5-flash",
            "gemini-1.0-pro-002",
            "gemini-1.0-pro-001",
            "gemini-1.0-pro",
            "gemini-pro"
        ]
        
        self.model = None
        for model_name in models_to_try:
            try:
                print(f"🔍 Tentando modelo: {model_name}...")
                test_model = GenerativeModel(model_name)
                # Teste rápido
                test_model.generate_content("test", generation_config=GenerationConfig(max_output_tokens=5))
                self.model = test_model
                print(f"✅ Usando modelo: {model_name}")
                break
            except Exception as e:
                if "404" in str(e):
                    print(f"   ❌ Modelo {model_name} não disponível")
                    continue
                else:
                    print(f"   ⚠️  Erro: {str(e)[:100]}")
                    continue
        
        if not self.model:
            raise RuntimeError(
                "❌ Nenhum modelo Gemini disponível no projeto.\n"
                "Verifique:\n"
                "1. Billing está habilitado\n"
                "2. APIs estão habilitadas\n"
                "3. Projeto tem acesso aos modelos Gemini\n"
                "4. Aguarde 5-10 minutos após habilitar as APIs"
            )
        
        self.generation_config = GenerationConfig(
            temperature=0.1,
            top_p=0.95,
            max_output_tokens=2048,
        )
        print(f"✅ Vertex AI inicializado (projeto: {project_id})")
    
    def infer_intent(self, user_query: str, context: Dict) -> Dict:
        """
        Infere intent com Gemini
        
        Args:
            user_query: pergunta do usuário
            context: contexto com flow, table, columns, ddl
            
        Returns:
            Dict com filters, select_columns, order_by, limit, confidence_score
        """
        prompt = self._build_prompt(user_query, context)
        
        response = self.model.generate_content(
            prompt,
            generation_config=self.generation_config
        )
        
        response_text = response.text.strip()
        
        # Remove markdown
        if response_text.startswith("```"):
            response_text = response_text.replace("```json", "").replace("```", "").strip()
        
        return json.loads(response_text)
    
    def _build_prompt(self, user_query: str, context: Dict) -> str:
        """Constrói prompt para o Gemini"""
        return f"""Você é um assistente SQL especializado em Sybase.

USER QUERY: "{user_query}"

CONTEXT:
{json.dumps(context, indent=2, ensure_ascii=False)}

Analise o user query e identifique:
1. Quais FILTROS (WHERE) aplicar
2. Quais COLUNAS retornar (SELECT) - se não especificado, deixe vazio []
3. Qual ORDENAÇÃO (ORDER BY)
4. Qual LIMITE de registros

IMPORTANTE:
- Para datas use funções Sybase: DATEADD(day, -N, GETDATE())
- Para aliases, mapeie para valores corretos
- Operadores válidos: =, >, <, >=, <=, IN, LIKE, BETWEEN
- Se não tiver certeza, confidence baixo

Retorne APENAS JSON (sem markdown):
{{
  "filters": [
    {{
      "column": "string",
      "operator": "string",
      "value": "any",
      "nl_term": "string",
      "confidence": 0.0-1.0
    }}
  ],
  "select_columns": [],
  "order_by": [{{"column": "string", "direction": "ASC|DESC"}}],
  "limit": 3,
  "confidence_score": 0.0-1.0,
  "reasoning": "string"
}}"""
