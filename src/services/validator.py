"""
Validator Service
=================
Serviço de validação em 3 camadas para dados não encontrados.
"""

from typing import List, Dict, Optional, Tuple
from difflib import SequenceMatcher
from ..models.intent import ValidationWarning, ValidationLevel, ProcessStatus


class ValidationService:
    """Serviço de validação em 3 camadas"""
    
    def __init__(self, repository):
        """
        Args:
            repository: repositório de metadados (Firestore)
        """
        self.repo = repository
    
    def validate_flow_and_table(self, flow_id: str, table_name: str) -> Tuple[bool, List[ValidationWarning]]:
        """
        CAMADA 1: Valida se flow e tabela existem (CRÍTICO)
        
        Args:
            flow_id: ID do flow
            table_name: nome da tabela
            
        Returns:
            (existe, warnings)
        """
        warnings = []
        
        # Validar flow
        flow_data = self.repo.get_flow(flow_id)
        if not flow_data:
            # Flow não existe - CRÍTICO
            all_flows = self.repo.get_all_flows()
            suggestions = self._fuzzy_match(flow_id, list(all_flows.keys()), threshold=0.6)
            
            warnings.append(ValidationWarning(
                level=ValidationLevel.CRITICAL,
                category='flow',
                message=f"Flow '{flow_id}' não encontrado no Firestore",
                details={'requested': flow_id, 'available_count': len(all_flows)},
                suggestions=suggestions[:3]
            ))
            return False, warnings
        
        # Validar tabela
        table_data = self.repo.get_table(flow_id, table_name)
        if not table_data:
            # Tabela não existe - CRÍTICO
            all_tables = self.repo.get_tables_by_flow(flow_id)
            table_names = [t.get('table_profile', {}).get('table_name', '') for t in all_tables]
            suggestions = self._fuzzy_match(table_name, table_names, threshold=0.6)
            
            warnings.append(ValidationWarning(
                level=ValidationLevel.CRITICAL,
                category='table',
                message=f"Tabela '{table_name}' não encontrada no flow '{flow_id}'",
                details={'requested': table_name, 'available': table_names},
                suggestions=suggestions[:3]
            ))
            return False, warnings
        
        return True, warnings
    
    def validate_columns(self, filters: List, ddl_columns: List[Dict],
                         main_table: str = "") -> Tuple[List, List[ValidationWarning]]:
        """
        CAMADA 2: Valida se colunas existem no DDL (WARNING).
        Filtros que referenciam tabelas relacionadas (schema.OutraTabela.col)
        são aceitos sem validação — o agente de query resolve o JOIN.
        """
        warnings = []
        validated_filters = []

        available_columns = {col['name'].lower(): col for col in ddl_columns}
        column_names = list(available_columns.keys())

        for filter_obj in filters:
            col = filter_obj.column

            # Detectar se é referência a tabela relacionada
            # Aceita 3 partes (Schema.Tabela.Coluna) ou 4 partes (DB.Schema.Tabela.Coluna)
            parts = col.split(".")
            if len(parts) >= 3:
                ref_table = parts[-2].lower()  # penúltima parte é sempre a tabela
                main_lower = main_table.lower() if main_table else ""
                if ref_table != main_lower:
                    # Coluna de tabela relacionada — aceita sem validar
                    filter_obj.validated = True
                    validated_filters.append(filter_obj)
                    continue

            # Coluna simples ou da tabela principal — valida contra DDL
            col_simple = parts[-1].lower()  # pega só o nome da coluna
            if col_simple in available_columns or col.lower() in available_columns:
                filter_obj.validated = True
                validated_filters.append(filter_obj)
            else:
                suggestions = self._fuzzy_match(col, column_names, threshold=0.6)
                warnings.append(ValidationWarning(
                    level=ValidationLevel.WARNING,
                    category='column',
                    message=f"Coluna '{col}' não encontrada no DDL - filtro ignorado",
                    details={
                        'requested': col,
                        'nl_term': filter_obj.nl_term,
                        'available_columns': column_names
                    },
                    suggestions=suggestions[:3]
                ))
                print(f"⚠️  Coluna '{col}' não encontrada - ignorando filtro")

        return validated_filters, warnings
    
    def validate_select_columns(self, select_columns: List[str], 
                               ddl_columns: List[Dict]) -> Tuple[List[str], List[ValidationWarning]]:
        """
        CAMADA 2: Valida colunas de retorno (WARNING)
        
        Args:
            select_columns: colunas solicitadas para retorno
            ddl_columns: colunas disponíveis no DDL
            
        Returns:
            (colunas_validadas, warnings)
        """
        warnings = []
        validated_columns = []
        
        # Criar mapa de colunas disponíveis
        available_columns = {col['name'].lower(): col['name'] for col in ddl_columns}
        column_names = list(available_columns.keys())
        
        for col in select_columns:
            col_lower = col.lower()
            
            if col_lower in available_columns:
                # Coluna existe - OK
                validated_columns.append(available_columns[col_lower])
            else:
                # Coluna não existe - WARNING
                suggestions = self._fuzzy_match(col, column_names, threshold=0.6)
                
                warnings.append(ValidationWarning(
                    level=ValidationLevel.WARNING,
                    category='column',
                    message=f"Coluna de retorno '{col}' não encontrada no DDL - ignorada",
                    details={
                        'requested': col,
                        'available_columns': column_names
                    },
                    suggestions=suggestions[:3]
                ))
        
        return validated_columns, warnings
    
    def calculate_status_and_confidence(self, warnings: List[ValidationWarning],
                                       base_confidence: float) -> Tuple[ProcessStatus, float]:
        """
        Calcula status final e ajusta confidence baseado em warnings
        
        Args:
            warnings: lista de warnings
            base_confidence: confidence inicial
            
        Returns:
            (status, confidence_ajustado)
        """
        if not warnings:
            return ProcessStatus.SUCCESS, base_confidence
        
        # Verificar se tem warnings críticos
        has_critical = any(w.level == ValidationLevel.CRITICAL for w in warnings)
        if has_critical:
            return ProcessStatus.ERROR, 0.0
        
        # Tem warnings mas não críticos
        warning_count = sum(1 for w in warnings if w.level == ValidationLevel.WARNING)
        
        # Reduzir confidence baseado em warnings
        confidence_penalty = min(0.3, warning_count * 0.1)
        adjusted_confidence = max(0.3, base_confidence - confidence_penalty)
        
        return ProcessStatus.PARTIAL_SUCCESS, adjusted_confidence
    
    def _fuzzy_match(self, target: str, candidates: List[str], 
                     threshold: float = 0.6) -> List[str]:
        """
        Fuzzy matching para sugestões
        
        Args:
            target: string alvo
            candidates: lista de candidatos
            threshold: threshold de similaridade (0-1)
            
        Returns:
            Lista de sugestões ordenadas por similaridade
        """
        if not candidates:
            return []
        
        matches = []
        target_lower = target.lower()
        
        for candidate in candidates:
            if not candidate:
                continue
            
            candidate_lower = candidate.lower()
            ratio = SequenceMatcher(None, target_lower, candidate_lower).ratio()
            
            if ratio >= threshold:
                matches.append((candidate, ratio))
        
        # Ordenar por similaridade (maior primeiro)
        matches.sort(key=lambda x: x[1], reverse=True)
        
        return [match[0] for match in matches]
