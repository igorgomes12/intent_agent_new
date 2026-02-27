"""
Intent Models
=============
Modelos de dados para representar a intenção extraída do usuário.
"""

from dataclasses import dataclass, asdict, field
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum


class ValidationLevel(str, Enum):
    """Níveis de validação"""
    CRITICAL = "critical"  # Flow/Tabela não existe
    WARNING = "warning"    # Coluna não existe
    INFO = "info"          # Valor não existe (deixa banco decidir)


class ProcessStatus(str, Enum):
    """Status do processamento"""
    SUCCESS = "success"              # Tudo OK
    PARTIAL_SUCCESS = "partial_success"  # Warnings mas processou
    ERROR = "error"                  # Erro crítico


@dataclass
class ValidationWarning:
    """Representa um warning de validação"""
    level: ValidationLevel
    category: str  # 'flow', 'table', 'column', 'value'
    message: str
    details: Dict[str, Any]
    suggestions: List[str] = field(default_factory=list)


@dataclass
class FilterCondition:
    """Representa um filtro extraído do prompt"""
    column: str
    operator: str
    value: Any
    nl_term: str
    resolved_via: str
    confidence: float = 1.0
    validated: bool = True  # Se a coluna foi validada no DDL


@dataclass
class DDLReference:
    """Referência ao DDL usado para validação"""
    flow_id: str
    table_name: str
    schema: str
    ddl_hash: str
    columns_available: List[Dict]
    constraints: Dict
    validated_at: str


@dataclass
class IntentObject:
    """Objeto de saída do Agente de Intenção"""
    flow_id: str
    table_name: str
    intent_type: str
    filters: List[FilterCondition]
    select_columns: List[str]
    joins: List[Dict]
    order_by: List[Dict]
    limit: int
    confidence_score: float
    metadata: Dict
    ddl_reference: DDLReference
    sources_consulted: Dict
    original_prompt: str
    created_at: str
    status: ProcessStatus = ProcessStatus.SUCCESS
    warnings: List[ValidationWarning] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        """Converte para dicionário"""
        data = asdict(self)
        data['filters'] = [asdict(f) for f in self.filters]
        data['ddl_reference'] = asdict(self.ddl_reference)
        data['status'] = self.status.value if isinstance(self.status, ProcessStatus) else self.status
        data['warnings'] = [
            {
                'level': w.level.value if isinstance(w.level, ValidationLevel) else w.level,
                'category': w.category,
                'message': w.message,
                'details': w.details,
                'suggestions': w.suggestions
            }
            for w in self.warnings
        ]
        return data
    
    def to_json(self, indent: int = 2) -> str:
        """Converte para JSON string"""
        import json
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    def to_output(self, repository=None, indent: int = 2) -> str:
        """
        Converte para o contrato esperado pelo agente de query:
        { parameters: { filter_fields, return_fields }, ddl: { ... } }

        Args:
            repository: FirestoreFirebaseRepository (opcional).
                        Se fornecido, inclui tabelas relacionadas no DDL.
        """
        import json
        from .query_output import convert_intent_to_query_format
        return json.dumps(
            convert_intent_to_query_format(self, repository=repository),
            indent=indent,
            ensure_ascii=False
        )
