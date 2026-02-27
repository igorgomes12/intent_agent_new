"""
AI Inference Service - Versão Google AI Studio (Nova API)
==========================================================
Usa a nova biblioteca google.genai com modelo gemini-2.5-flash
"""

import json
import os
from typing import Dict
from google import genai
from google.genai import types


class AIInferenceServiceGemini:
    """Serviço de inferência com Google AI Studio (Nova API)"""
    
    def __init__(self, api_key: str = None):
        """
        Args:
            api_key: API Key do Google AI Studio
                    Se None, tenta ler de GOOGLE_API_KEY env var
        """
        if api_key is None:
            api_key = os.getenv('GOOGLE_API_KEY')
            if not api_key:
                raise ValueError(
                    "API Key não fornecida!\n"
                    "Opções:\n"
                    "1. Passe api_key no construtor\n"
                    "2. Defina variável GOOGLE_API_KEY no .env\n"
                    "3. Crie API Key em: https://aistudio.google.com/app/apikey"
                )
        
        # Cliente com api_key apenas — sem credenciais GCP para não rotear para Vertex
        self.client = genai.Client(api_key=api_key)
        os.environ.pop('GOOGLE_CLOUD_PROJECT', None)
        os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)

        self.generation_config = types.GenerateContentConfig(
            temperature=0.1,
            top_p=0.95,
            max_output_tokens=8192,  # Aumentado para evitar truncamento
        )

        # Tenta modelos em ordem até encontrar um com quota disponível
        candidates = [
            "models/gemini-3.1-pro-preview",
            "models/gemini-3-pro-preview",
            "models/gemini-2.5-flash",
            "models/gemini-3-flash-preview",
            "models/gemini-2.5-flash-lite",
            "models/gemini-2.0-flash",
            "models/gemini-2.0-flash-lite",
        ]
        self.model_name = None
        for m in candidates:
            try:
                self.client.models.generate_content(model=m, contents="ok",
                                                    config=self.generation_config)
                self.model_name = m
                break
            except Exception as e:
                if "429" in str(e):
                    print(f"   ⚠️  {m} — quota esgotada, tentando próximo...")
                    continue
                elif "404" in str(e):
                    continue
                raise

        if not self.model_name:
            raise RuntimeError("❌ Quota esgotada em todos os modelos Gemini disponíveis.")

        print(f"✅ Google AI Studio inicializado (modelo: {self.model_name})")
    
    def infer_table_selection(self, prompt: str) -> Dict:
        """Mantido por compatibilidade."""
        return self._call_with_retry(prompt)

    def infer_scan(self, user_query: str, flow_id: str, catalog: list,
                   dictionary_block: str = "") -> Dict:
        """Chamada ÚNICA: seleciona tabela + extrai filtros."""
        from .prompt_builder import build_scan_prompt
        return self._call_with_retry(
            build_scan_prompt(user_query, flow_id, catalog, dictionary_block)
        )

    def infer_intent(self, user_query: str, context: Dict) -> Dict:
        """Infere intent com Gemini"""
        prompt = self._build_prompt(user_query, context)
        return self._call_with_retry(prompt)

    def _call_with_retry(self, prompt: str, max_retries: int = 3) -> Dict:
        """Chama o modelo com retry automático em caso de rate limit (429)."""
        import time
        import re

        for attempt in range(max_retries):
            try:
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=prompt,
                    config=self.generation_config,
                )
                text = response.text.strip()

                # Remove blocos markdown
                if "```" in text:
                    text = re.sub(r"```(?:json)?", "", text).strip()

                # Remove comentários // e /* */
                text = re.sub(r"//.*", "", text)
                text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)

                # Remove trailing commas antes de } ou ]
                text = re.sub(r",\s*([}\]])", r"\1", text)

                try:
                    return json.loads(text)
                except json.JSONDecodeError as je:
                    # Loga o trecho problemático para diagnóstico
                    lines = text.splitlines()
                    err_line = je.lineno - 1
                    snippet = "\n".join(lines[max(0, err_line-2):err_line+3])
                    print(f"⚠️  JSON inválido na linha {je.lineno}: {je.msg}")
                    print(f"   Trecho:\n{snippet}")
                    
                    # Tenta recuperar JSON parcial se possível
                    if "Unterminated string" in je.msg or "Expecting" in je.msg:
                        print(f"   💡 Tentando recuperar JSON parcial...")
                        # Tenta encontrar o último objeto JSON válido
                        for i in range(len(text), 0, -1):
                            try:
                                partial = text[:i].rstrip() + '"}]}'
                                result = json.loads(partial)
                                print(f"   ✅ JSON parcial recuperado")
                                return result
                            except:
                                continue
                    raise

            except json.JSONDecodeError:
                raise
            except Exception as e:
                err = str(e)
                if "429" in err:
                    match = re.search(r'retry in (\d+)', err)
                    wait = int(match.group(1)) + 2 if match else 30 * (attempt + 1)
                    print(f"⏳ Rate limit. Aguardando {wait}s... (tentativa {attempt+1}/{max_retries})")
                    time.sleep(wait)
                else:
                    raise

        raise RuntimeError(f"❌ Falha após {max_retries} tentativas.")
    
    def _build_prompt(self, user_query: str, context: Dict) -> str:
        """Constrói prompt para o Gemini"""
        # Monta lista legível de colunas disponíveis
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
