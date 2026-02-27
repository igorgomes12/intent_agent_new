"""
Filter Extractor Service
========================
Responsável por extrair filtros do prompt usando heurísticas.
"""

import re
from typing import Optional, List, Dict
from ..models.intent import FilterCondition


class FilterExtractor:
    """Serviço para extrair filtros usando heurísticas"""
    
    def __init__(self, repository):
        self.repo = repository
    
    def extract_from_columns(self, prompt: str, columns: List[Dict],
                            ddl_columns: List[Dict], flow_data: Dict) -> List[FilterCondition]:
        """
        Extrai filtros das colunas candidatas
        
        Args:
            prompt: prompt do usuário
            columns: colunas candidatas do metadata
            ddl_columns: colunas do DDL
            flow_data: dados do flow
            
        Returns:
            Lista de FilterCondition
        """
        filters = []
        
        for col in columns:
            filter_cond = self._extract_filter_heuristic(
                prompt, col, ddl_columns, flow_data
            )
            if filter_cond:
                filters.append(filter_cond)
        
        return filters
    
    def get_candidate_columns(self, flow_id: str, table_name: str, 
                             query: str) -> List[Dict]:
        """
        Retorna colunas candidatas a filtro baseado no query
        
        Args:
            flow_id: flow identificado
            table_name: tabela identificada
            query: prompt do usuário
            
        Returns:
            Lista de metadados de colunas com score
        """
        query_lower = query.lower()
        table_data = self.repo.get_table(flow_id, table_name)
        
        if not table_data:
            return []
        
        columns_dict = table_data.get('columns_dictionary', [])
        candidates = []
        
        for col in columns_dict:
            score = self._calculate_column_score(query_lower, col)
            
            if score > 0:
                col_with_score = col.copy()
                col_with_score['_match_score'] = score
                candidates.append(col_with_score)
        
        candidates.sort(key=lambda x: x.get('_match_score', 0), reverse=True)
        return candidates[:10]
    
    def _calculate_column_score(self, query_lower: str, col: Dict) -> int:
        """Calcula score de relevância da coluna"""
        score = 0

        # Nome da coluna (normalizado)
        col_name = col.get('name', col.get('column_name', '')).lower()
        if col_name and col_name in query_lower:
            score += 10

        # Partes do nome da coluna (ex: "Weight" em "ProductWeight")
        for part in self._split_camel(col_name):
            if len(part) > 2 and part in query_lower:
                score += 5

        # nl_terms (estrutura antiga, mantido por compatibilidade)
        nl_terms = col.get('ai_hints', {}).get('nl_terms_seed', [])
        for term in nl_terms:
            if term.lower() in query_lower:
                score += 5

        # Description
        description = col.get('description', '').lower()
        if description and any(word in description for word in query_lower.split()):
            score += 1

        return score

    def _split_camel(self, name: str) -> List[str]:
        """Divide CamelCase em partes: 'ProductWeight' → ['product', 'weight']"""
        import re
        parts = re.sub(r'([A-Z])', r'_\1', name).lower().split('_')
        return [p for p in parts if p]
    
    def _extract_filter_heuristic(self, prompt: str, col: Dict,
                                  ddl_columns: List[Dict], 
                                  flow_data: Dict) -> Optional[FilterCondition]:
        """Extrai filtro usando heurísticas simples"""
        prompt_lower = prompt.lower()
        col_name = col['name']
        
        # Validar que coluna existe no DDL
        ddl_col = next(
            (c for c in ddl_columns if c['name'] == col_name),
            None
        )
        
        if not ddl_col:
            return None
        
        # Heurística 1: Temporal
        if 'datetime' in ddl_col['type'].lower():
            temporal_filter = self._extract_temporal_filter(prompt_lower, col_name)
            if temporal_filter:
                return temporal_filter
        
        # Heurística 2: Alias
        alias_filter = self._extract_alias_filter(
            prompt_lower, col, flow_data
        )
        if alias_filter:
            return alias_filter
        
        # Heurística 3: Status
        if 'status' in col_name.lower():
            status_filter = self._extract_status_filter(prompt_lower, col_name)
            if status_filter:
                return status_filter
        
        return None
    
    def _extract_temporal_filter(self, prompt_lower: str, 
                                col_name: str) -> Optional[FilterCondition]:
        """Extrai filtro temporal"""
        if not any(term in prompt_lower for term in ['últim', 'dias', 'semanas', 'mês', 'mes']):
            return None
        
        days_match = re.search(r'(\d+)\s*dias?', prompt_lower)
        if days_match:
            days = int(days_match.group(1))
            return FilterCondition(
                column=col_name,
                operator=">=",
                value=f"DATEADD(day, -{days}, GETDATE())",
                nl_term=f"últimos {days} dias",
                resolved_via="heuristic_temporal"
            )
        
        return None
    
    def _extract_alias_filter(self, prompt_lower: str, col: Dict,
                             flow_data: Dict) -> Optional[FilterCondition]:
        """Extrai filtro baseado em alias"""
        nl_terms = col.get('ai_hints', {}).get('nl_terms_seed', [])
        
        for term in nl_terms:
            if term.lower() not in prompt_lower:
                continue
            
            # Buscar alias no flow
            alias = self._search_alias(term, flow_data)
            if alias:
                return FilterCondition(
                    column=col['name'],
                    operator="=",
                    value=alias.get('resolved_value', term),
                    nl_term=alias['canonical'],
                    resolved_via="alias_match"
                )
        
        return None
    
    def _extract_status_filter(self, prompt_lower: str,
                              col_name: str) -> Optional[FilterCondition]:
        """Extrai filtro de status"""
        for status in ['aprovada', 'recusada', 'análise', 'pendente']:
            if status in prompt_lower:
                return FilterCondition(
                    column=col_name,
                    operator="=",
                    value=status.capitalize(),
                    nl_term=status,
                    resolved_via="heuristic_status"
                )
        
        return None
    
    def _search_alias(self, term: str, flow_data: Dict) -> Optional[Dict]:
        """Busca se um termo é um alias conhecido"""
        term_lower = term.lower()
        
        for alias in flow_data.get('aliases', {}).get('seed', []):
            if alias.get('canonical', '').lower() == term_lower:
                return alias
            
            for variant in alias.get('variants', []):
                if variant.lower() == term_lower:
                    return alias
        
        return None
