"""
Local JSON Repository
=====================
Repositório local que lê a estrutura do banco a partir de um arquivo JSON.
Simula a estrutura do Firestore (fluxos_automotivos) sem precisar de conexão.

Estrutura esperada no JSON:
  {
    "FLUXO_PROPOSTA_VEICULO_CRM": {
      "flow_metadata": { ... },
      "tables": [ { "table_definition": { ... } }, ... ]
    }
  }

Mapeamento:
  flow_id    → chave do documento (ex: "FLUXO_PROPOSTA_VEICULO_CRM")
  table_name → table_definition.table_name dentro do array tables[]
"""

import json
import os
from typing import Dict, List, Optional


class LocalJsonRepository:
    """Repositório local que lê dados de um arquivo JSON."""

    def __init__(self, json_path: str = None):
        if json_path is None:
            json_path = os.path.join(
                os.path.dirname(__file__), "..", "data", "fluxos_automotivos.json"
            )
        with open(json_path, encoding="utf-8") as f:
            raw_data = json.load(f)
        
        # Nova estrutura: __collections__.fluxos_automotivos.{FLOW_ID}
        if "__collections__" in raw_data:
            collections = raw_data["__collections__"]
            if "fluxos_automotivos" in collections:
                self._data = collections["fluxos_automotivos"]
            else:
                self._data = {}
        else:
            # Fallback para estrutura antiga
            self._data = raw_data
            
        print(f"📂 Repositório local carregado: {json_path}")
        print(f"   Flows disponíveis: {list(self._data.keys())}")

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _get_doc(self, flow_id: str) -> Optional[Dict]:
        return self._data.get(flow_id)

    def _normalize_columns(self, raw: list) -> List[Dict]:
        columns = []
        for col in raw:
            if not isinstance(col, dict):
                continue
            columns.append({
                "column_name": col.get("name", ""),
                "name": col.get("name", ""),
                "data_type": col.get("type", "string"),
                "type": col.get("type", "string"),
                "nullable": col.get("nullable", True),
                "is_key_field": col.get("is_key_field", False),
                "description": col.get("description", ""),
            })
        return columns

    def _table_def_to_table(self, flow_id: str, table_def: Dict) -> Dict:
        columns = self._normalize_columns(table_def.get("columns", []))
        database = table_def.get("database", "")
        schema = table_def.get("schema", "dbo")
        table_name = table_def.get("table_name", "")

        return {
            "flow_id": flow_id,
            "table_profile": {
                "table_name": table_name,
                "display_name": table_name,
                "description": table_def.get("description", ""),
                "schema": schema,
                "database": database,
            },
            "columns_dictionary": columns,
            "relationships": {},
            "source": "local_json",
            "original_data": {"table_definition": table_def},
        }

    # ------------------------------------------------------------------
    # API pública (mesma interface do FirestoreFirebaseRepository)
    # ------------------------------------------------------------------

    def get_flow(self, flow_id: str) -> Optional[Dict]:
        doc = self._get_doc(flow_id)
        if not doc:
            return None
        meta = doc.get("flow_metadata", {})
        return {
            "flow_id": flow_id,
            "description": meta.get("description", ""),
            "display_name": meta.get("display_name", flow_id),
            "source": "local_json",
        }

    def get_all_flows(self) -> Dict[str, Dict]:
        return {
            flow_id: self.get_flow(flow_id)
            for flow_id in self._data
        }

    def get_tables_by_flow(self, flow_id: str) -> List[Dict]:
        doc = self._get_doc(flow_id)
        if not doc:
            return []
        tables = []
        for entry in doc.get("tables", []):
            table_def = entry.get("table_definition", {})
            if table_def:
                tables.append(self._table_def_to_table(flow_id, table_def))
        return tables

    def get_table(self, flow_id: str, table_name: str) -> Optional[Dict]:
        doc = self._get_doc(flow_id)
        if not doc:
            return None
        for entry in doc.get("tables", []):
            table_def = entry.get("table_definition", {})
            if table_def.get("table_name", "").lower() == table_name.lower():
                return self._table_def_to_table(flow_id, table_def)
        return None

    def get_ddl(self, flow_id: str, table_name: str) -> Optional[Dict]:
        doc = self._get_doc(flow_id)
        if not doc:
            return None

        table_def = None
        for entry in doc.get("tables", []):
            td = entry.get("table_definition", {})
            if td.get("table_name", "").lower() == table_name.lower():
                table_def = td
                break

        if not table_def:
            return None

        columns = self._normalize_columns(table_def.get("columns", []))
        constraints = table_def.get("constraints", {})

        pk_data = constraints.get("primary_key", {})
        primary_key = pk_data.get("columns", []) if isinstance(pk_data, dict) else pk_data

        raw_fks = constraints.get("foreign_key_hints", [])
        foreign_keys = []
        for fk in raw_fks:
            # foreign_key_hints: from_table é a tabela referenciada, to_table é a tabela atual
            # Corrigindo a interpretação: from_table → references.table
            foreign_keys.append({
                "name": fk.get("relationship_name", ""),
                "column": fk.get("column", ""),
                "references": {
                    "table": fk.get("from_table", ""),  # Tabela referenciada
                    "column": fk.get("column", ""),
                },
            })

        return {
            "flow_id": flow_id,
            "database": table_def.get("database", ""),
            "table_name": table_def.get("table_name", table_name),
            "schema": table_def.get("schema", "dbo"),
            "columns": [
                {"name": c["name"], "type": c["type"], "nullable": c["nullable"]}
                for c in columns
            ],
            "constraints": {
                "primary_key": primary_key,
                "foreign_keys": foreign_keys,
            },
        }
