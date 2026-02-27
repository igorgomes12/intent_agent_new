"""
AI Strategy
===========
Estratégia que usa Vertex AI Gemini para inferência.
"""

import hashlib
from datetime import datetime
from typing import Dict
from .base import IntentStrategy
from ..models.intent import IntentObject, FilterCondition, DDLReference
from ..services.ai_inference import AIInferenceService


class AIStrategy(IntentStrategy):
    """Estratégia usando Gemini"""
    
    def __init__(self, ai_service: AIInferenceService):
        self.ai_service = ai_service
    
    def build_intent(self, user_prompt: str, flow_data: Dict,
                    table_data: Dict, ddl_data: Dict) -> IntentObject:
        """Usa Gemini quando busca local não é suficiente"""

        # Carregar DDL das tabelas relacionadas via FK
        related_ddls = self._load_related_ddls(flow_data, ddl_data)

        # Montar contexto com tabelas relacionadas incluídas
        context = self._build_context(flow_data, table_data, ddl_data, related_ddls)

        # Chamar Gemini
        gemini_result = self.ai_service.infer_intent(user_prompt, context)

        print(f"🤖 Gemini confidence: {gemini_result.get('confidence_score', 0):.2f}")

        # Criar DDL Reference
        ddl_ref = self._create_ddl_reference(
            flow_data['flow_id'],
            table_data['table_profile']['table_name'],
            ddl_data
        )

        # Converter para FilterCondition
        filters = [
            FilterCondition(
                column=f['column'],
                operator=f['operator'],
                value=f['value'],
                nl_term=f.get('nl_term', ''),
                resolved_via='gemini_inference',
                confidence=f.get('confidence', 0.8)
            )
            for f in gemini_result.get('filters', [])
        ]

        metadata = self._build_metadata(flow_data, ddl_ref, gemini_result)

        return IntentObject(
            flow_id=flow_data['flow_id'],
            table_name=table_data['table_profile']['table_name'],
            intent_type='query',
            filters=filters,
            select_columns=gemini_result.get('select_columns', []),
            joins=[],
            order_by=gemini_result.get('order_by', []),
            limit=gemini_result.get('limit', 0),
            confidence_score=gemini_result.get('confidence_score', 0.7),
            metadata=metadata,
            ddl_reference=ddl_ref,
            sources_consulted={"ddl": True, "gemini": True},
            original_prompt=user_prompt,
            created_at=datetime.now().isoformat()
        )

    def _load_related_ddls(self, flow_data: Dict, ddl_data: Dict) -> Dict[str, Dict]:
        """
        Carrega DDL das tabelas referenciadas nas foreign keys.
        Retorna dict {table_name: ddl_data}.
        """
        related = {}
        repo = getattr(self, '_repo', None)
        if not repo:
            return related

        flow_id = flow_data.get('flow_id', '')
        schema = ddl_data.get('schema', '')

        for fk in ddl_data.get('constraints', {}).get('foreign_keys', []):
            ref_table = fk.get('references', {}).get('table', '')
            if not ref_table or ref_table in related:
                continue

            # Tenta pelo ID do documento: TABLE_{SCHEMA}_{TABLE}
            doc_id = f"TABLE_{schema.upper()}_{ref_table.upper()}" if schema else f"TABLE_{ref_table.upper()}"
            rel_ddl = repo.get_ddl(flow_id, doc_id)
            if rel_ddl:
                related[ref_table] = rel_ddl

        return related
    
    def _build_context(self, flow_data: Dict, table_data: Dict,
                      ddl_data: Dict, related_ddls: Dict = None) -> Dict:
        """Constrói contexto para o Gemini incluindo tabelas relacionadas"""
        schema = ddl_data.get('schema', '')
        main_table = table_data['table_profile']['table_name']

        # Colunas da tabela principal
        main_cols = [
            {
                "table": main_table,
                "column": c["name"],
                "type": c["type"],
                "full_ref": f"{schema}.{main_table}.{c['name']}" if schema else f"{main_table}.{c['name']}"
            }
            for c in ddl_data.get('columns', [])
        ]

        # Colunas das tabelas relacionadas
        related_cols = []
        related_tables_info = []
        for rel_name, rel_ddl in (related_ddls or {}).items():
            rel_schema = rel_ddl.get('schema', schema)
            rel_table  = rel_ddl.get('table_name', rel_name)
            for c in rel_ddl.get('columns', []):
                related_cols.append({
                    "table": rel_table,
                    "column": c["name"],
                    "type": c["type"],
                    "full_ref": f"{rel_schema}.{rel_table}.{c['name']}" if rel_schema else f"{rel_table}.{c['name']}"
                })
            related_tables_info.append({
                "table": rel_table,
                "schema": rel_schema,
                "joined_via": next(
                    (fk for fk in ddl_data.get('constraints', {}).get('foreign_keys', [])
                     if fk.get('references', {}).get('table') == rel_name),
                    {}
                )
            })

        return {
            "main_table": {"schema": schema, "name": main_table},
            "related_tables": related_tables_info,
            "all_available_columns": main_cols + related_cols,
            "foreign_keys": ddl_data.get('constraints', {}).get('foreign_keys', []),
        }
    
    def _create_ddl_reference(self, flow_id: str, table_name: str,
                             ddl_data: Dict) -> DDLReference:
        """Cria referência DDL"""
        ddl_hash = hashlib.sha256(
            str(ddl_data).encode()
        ).hexdigest()[:16]
        
        return DDLReference(
            flow_id=flow_id,
            table_name=table_name,
            schema=ddl_data.get('schema', 'dbo'),
            ddl_hash=ddl_hash,
            columns_available=ddl_data.get('columns', []),
            constraints=ddl_data.get('constraints', {}),
            validated_at=datetime.now().isoformat()
        )
    
    def _build_metadata(self, flow_data: Dict, ddl_ref: DDLReference,
                       gemini_result: Dict) -> Dict:
        """Constrói metadados"""
        return {
            "schema": ddl_ref.schema,
            "grain_keys": flow_data.get('entities', {}).get('grain_keys', []),
            "blocked_columns": flow_data.get('return_expected', {}).get('blocked_columns', []),
            "database_type": flow_data.get('database', {}).get('type', 'SYBASE'),
            "database_dialect": flow_data.get('database', {}).get('dialect', 'tsql_sybase'),
            "gemini_reasoning": gemini_result.get('reasoning', '')
        }
