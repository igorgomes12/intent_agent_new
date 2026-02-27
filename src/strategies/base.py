"""
Base Strategy
=============
Interface para estratégias de extração de intenção.
"""

from abc import ABC, abstractmethod
from typing import Dict
from ..models.intent import IntentObject


class IntentStrategy(ABC):
    """Interface para estratégias de extração de intenção"""
    
    @abstractmethod
    def build_intent(self, user_prompt: str, flow_data: Dict,
                    table_data: Dict, ddl_data: Dict) -> IntentObject:
        """
        Constrói IntentObject usando a estratégia específica
        
        Args:
            user_prompt: prompt do usuário
            flow_data: dados do flow
            table_data: dados da tabela
            ddl_data: dados do DDL
            
        Returns:
            IntentObject
        """
        pass
