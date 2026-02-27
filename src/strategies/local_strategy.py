"""
Local Strategy
==============
Estratégia que usa busca local e heurísticas.
"""

import hashlib
from datetime import datetime
from typing import Dict
from .base import IntentStrategy
from ..models.intent import IntentObject, DDLReference
from ..services.filter_extractor import FilterExtractor


class LocalStrategy(IntentStrategy):
    """Estratégia de busca local"""
    
    def __init__(self, filter_extractor: FilterExtractor):
        self.filter_extractor = filter_extractor
    
    def build_intent(self, user_prompt: str, flow_data: Dict,
                    table_data: Dict, ddl_data: Dict) -> IntentObject:
        """Constrói intent usando busca local"""
        
        flow_id = flow_data['flow_id']
        table_name = table_data['table_profile']['table_name']
        
        # Criar DDL Reference
        ddl_ref = self._create_ddl_reference(flow_id, table_name, ddl_data)
        
        # Buscar colunas candidatas
        columns = self.filter_extractor.get_candidate_columns(
            flow_id, table_name, user_prompt
        )
        
        # Extrair filtros
        filters = self.filter_extractor.extract_from_columns(
            user_prompt, columns, ddl_ref.columns_available, flow_data
        )
        
        for f in filters:
            print(f"  📌 Filtro: {f.column} {f.operator} {f.value}")
        
        # Metadados
        metadata = self._build_metadata(flow_data, ddl_ref)
        
        return IntentObject(
            flow_id=flow_id,
            table_name=table_name,
            intent_type=flow_data.get('return_expected', {}).get('purpose', 'massa_para_teste'),
            filters=filters,
            select_columns=[],
            joins=[],
            order_by=flow_data.get('return_expected', {}).get('sorting_preference', []),
            limit=flow_data.get('return_expected', {}).get('limit_default', 3),
            confidence_score=0.75,
            metadata=metadata,
            ddl_reference=ddl_ref,
            sources_consulted={
                "flow_metadata": True,
                "table_profile": True,
                "ddl": True,
                "strategy": "local"
            },
            original_prompt=user_prompt,
            created_at=datetime.now().isoformat()
        )
    
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
    
    def _build_metadata(self, flow_data: Dict, ddl_ref: DDLReference) -> Dict:
        """Constrói metadados"""
        return {
            "schema": ddl_ref.schema,
            "grain_keys": flow_data.get('entities', {}).get('grain_keys', []),
            "blocked_columns": flow_data.get('return_expected', {}).get('blocked_columns', []),
            "database_type": flow_data.get('database', {}).get('type', 'SYBASE'),
            "database_dialect": flow_data.get('database', {}).get('dialect', 'tsql_sybase')
        }
