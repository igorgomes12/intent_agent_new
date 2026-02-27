"""
AI Inference Service - Vertex AI (Nova API)
============================================
Usa a nova biblioteca google.genai com Vertex AI
"""

import json
import os
from typing import Dict
from google import genai
from google.genai import types


class AIInferenceServiceVertex:
    """Serviço de inferência com Vertex AI (nova API google.genai)"""
    
    def __init__(self, project_id: str, location: str = "us-central1", 
                 credentials_path: str = None):
        """
        Args:
            project_id: ID do projeto GCP
            location: região (us-central1, etc)
            credentials_path: caminho para arquivo JSON de credenciais
        """
        # Configurar credenciais
        if credentials_path and os.path.exists(credentials_path):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        
        # Criar cliente Vertex AI
        self.client = genai.Client(
            vertexai=True,
            project=project_id,
            location=location
        )
        
        # Modelos disponíveis no Vertex AI (ordem de preferência)
        # Gemini 2.0 é a versão mais recente (não existe 3.0 ainda)
        models_to_try = [
            "gemini-2.0-flash-exp",      # Gemini 2.0 Flash (experimental)
            "gemini-2.0-flash",           # Gemini 2.0 Flash (stable)
            "gemini-1.5-pro-002",         # Gemini 1.5 Pro (mais recente)
            "gemini-1.5-flash-002",       # Gemini 1.5 Flash (mais recente)
            "gemini-1.5-pro-001",         # Gemini 1.5 Pro
            "gemini-1.5-flash-001",       # Gemini 1.5 Flash
            "gemini-1.5-pro",             # Gemini 1.5 Pro (stable)
            "gemini-1.5-flash"            # Gemini 1.5 Flash (stable)
        ]
        
        self.model_name = None
        for model in models_to_try:
            try:
                # Testar se modelo está disponível
                print(f"🔍 Testando modelo {model}...")
                self.client.models.generate_content(
                    model=model,
                    contents="test"
                )
                self.model_name = model
                print(f"✅ Usando modelo: {model}")
                break
            except Exception as e:
                error_str = str(e)
                if "404" in error_str or "not found" in error_str.lower():
                    print(f"   ❌ Modelo {model} não disponível")
                    continue
                else:
                    print(f"   ⚠️  Erro ao testar {model}: {error_str[:100]}")
                    continue
        
        if not self.model_name:
            raise RuntimeError(
                "❌ Nenhum modelo Gemini disponível no Vertex AI.\n"
                "Verifique:\n"
                "1. Gemini for Google Cloud API está habilitada\n"
                "2. Billing está habilitado no projeto\n"
                "3. Região us-central1 tem acesso aos modelos\n"
                "4. Credenciais têm permissão para usar Vertex AI"
            )
        
        self.generation_config = types.GenerateContentConfig(
            temperature=0.1,
            top_p=0.95,
            max_output_tokens=2048,
        )
        
        print(f"✅ Vertex AI inicializado (projeto: {project_id}, região: {location})")
    
    def infer_table_selection(self, prompt: str) -> Dict:
        """Mantido por compatibilidade."""
        response = self.client.models.generate_content(
            model=self.model_name, contents=prompt, config=self.generation_config
        )
        text = response.text.strip()
        if text.startswith("```"):
            text = text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)

    def infer_scan(self, user_query: str, flow_id: str, catalog: list,
                   dictionary_block: str = "") -> Dict:
        """Chamada ÚNICA: seleciona tabela + extrai filtros."""
        from .prompt_builder import build_scan_prompt
        prompt_text = build_scan_prompt(user_query, flow_id, catalog, dictionary_block)
        response = self.client.models.generate_content(
            model=self.model_name, contents=prompt_text, config=self.generation_config
        )
        text = response.text.strip()
        if text.startswith("```"):
            text = text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)

    def infer_intent(self, user_query: str, context: Dict) -> Dict:
        """
        Infere intent com Gemini via Vertex AI
        
        Args:
            user_query: pergunta do usuário
            context: contexto com flow, table, columns, ddl
            
        Returns:
            Dict com filters, select_columns, order_by, limit, confidence_score
        """
        prompt = self._build_prompt(user_query, context)
        
        response = self.client.models.generate_content(
            model=self.model_name,
            contents=prompt,
            config=self.generation_config
        )
        
        response_text = response.text.strip()
        
        # Remove markdown
        if response_text.startswith("```"):
            response_text = response_text.replace("```json", "").replace("```", "").strip()
        
        return json.loads(response_text)
    
    def _build_prompt(self, user_query: str, context: Dict) -> str:
        """Constrói prompt para o Gemini"""
        cols_list = "\n".join(
            f"  - {c['full_ref']} ({c['type']})"
            for c in context.get("all_available_columns", [])
        )
        fks = context.get("foreign_keys", [])
        fk_list = "\n".join(
            f"  - {fk.get('column')} → {fk.get('references', {}).get('table')}.{fk.get('references', {}).get('column')}"
            for fk in fks
        ) or "  (nenhuma)"

        return f"""Você é um assistente que extrai filtros de linguagem natural para JSON estruturado.

USER QUERY: "{user_query}"

TABELA PRINCIPAL: {context.get('main_table', {}).get('schema', '')}.{context.get('main_table', {}).get('name', '')}

COLUNAS DISPONÍVEIS PARA FILTRO (use APENAS estas):
{cols_list}

FOREIGN KEYS (relacionamentos disponíveis):
{fk_list}

REGRAS OBRIGATÓRIAS:
1. Use SOMENTE colunas da lista acima — NUNCA invente colunas.
2. O campo "column" deve ser o "full_ref" exato da lista (ex: "SalesLT.ProductCategory.Name").
3. O campo "value" deve ser APENAS o valor literal: string, número ou expressão de data.
4. PROIBIDO usar subselects, subqueries ou SQL no campo "value".
5. Se o filtro for sobre uma categoria/nome de tabela relacionada, use a coluna dessa tabela relacionada diretamente.
   Exemplo correto:   {{"column": "SalesLT.ProductCategory.Name", "operator": "=", "value": "Mountain Bikes"}}
   Exemplo ERRADO:    {{"column": "SalesLT.Product.ProductCategoryID", "operator": "IN", "value": "(SELECT ...)"}}
6. Operadores válidos: =, >, <, >=, <=, IN, LIKE, BETWEEN
7. Para datas: DATEADD(day, -N, GETDATE())

Retorne APENAS JSON (sem markdown):
{{
  "filters": [
    {{
      "column": "full_ref exato da lista",
      "operator": "=",
      "value": "valor literal",
      "nl_term": "termo do usuário",
      "confidence": 0.0-1.0
    }}
  ],
  "select_columns": [],
  "order_by": [{{"column": "full_ref", "direction": "ASC|DESC"}}],
  "limit": 0,
  "confidence_score": 0.0-1.0,
  "reasoning": "string"
}}"""
