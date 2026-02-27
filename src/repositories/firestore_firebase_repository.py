"""
Firestore Firebase Repository
==============================
Repositório que acessa a estrutura do Firebase.
Adaptado para a nova estrutura: coleção = database, documento = tabela.

Estrutura esperada no Firestore:
  /{collection}/{TABLE_DOCUMENT}/
    - columns: [ { name, type, nullable, is_key_field }, ... ]
    - (outros campos opcionais: description, schema, etc.)

Mapeamento:
  flow_id   → nome da coleção  (ex: "adventureworks_lt")
  table_name → nome do documento (ex: "TABLE_SALESLT_PRODUCT")
"""

from typing import Dict, Optional, List
from google.cloud import firestore


class FirestoreFirebaseRepository:
    """Repositório que acessa dados do Firebase Firestore"""

    def __init__(self, project_id: str, database: str = "(default)",
                 credentials_path: str = None):
        if credentials_path:
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            self.db = firestore.Client(
                project=project_id,
                database=database,
                credentials=credentials
            )
        else:
            self.db = firestore.Client(project=project_id, database=database)

        print(f"🔥 Firebase Firestore Repository: {project_id}/{database}")
        if credentials_path:
            print(f"   Credenciais: {credentials_path}")

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _get_collection_ref(self, flow_id: str):
        """Retorna referência para a coleção (flow_id = nome da coleção)"""
        return self.db.collection(flow_id)

    def _doc_to_table(self, flow_id: str, doc) -> Dict:
        """Converte documento Firestore para formato interno de tabela"""
        data = doc.to_dict() or {}

        # Estrutura real: campos ficam dentro de sub-dicts
        table_def = data.get("table_definition", {})
        semantic  = data.get("semantic_profile", {})
        flow_meta = data.get("flow_metadata", {})

        raw_columns = table_def.get("columns", [])
        columns = self._normalize_columns(raw_columns)

        description = (
            semantic.get("description")
            or flow_meta.get("description")
            or table_def.get("display_name", "")
        )

        return {
            "flow_id": flow_id,
            "table_profile": {
                "table_name": table_def.get("table_name", doc.id),
                "display_name": table_def.get("display_name", doc.id),
                "description": description,
                "schema": table_def.get("schema", ""),
            },
            "columns_dictionary": columns,
            "relationships": data.get("ai_and_rag_support", {}).get("relationships_hints", {}),
            "source": "firebase",
            "original_data": data,
        }

    def _normalize_columns(self, raw) -> List[Dict]:
        """
        Aceita tanto lista quanto dict indexado (Firestore às vezes serializa
        arrays como {0: {...}, 1: {...}}).
        """
        if isinstance(raw, list):
            items = raw
        elif isinstance(raw, dict):
            # dict com chaves numéricas → ordena e extrai valores
            items = [raw[k] for k in sorted(raw.keys(), key=lambda x: int(x))]
        else:
            return []

        columns = []
        for col in items:
            if not isinstance(col, dict):
                continue
            columns.append({
                "column_name": col.get("name", ""),
                "name": col.get("name", ""),          # alias para compatibilidade
                "data_type": col.get("type", "string"),
                "type": col.get("type", "string"),    # alias para compatibilidade
                "nullable": col.get("nullable", True),
                "is_key_field": col.get("is_key_field", False),
                "description": col.get("description", ""),
            })
        return columns

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def get_flow(self, flow_id: str) -> Optional[Dict]:
        """
        Verifica se a coleção (flow_id) existe retornando metadados básicos.
        Uma coleção existe se tiver ao menos um documento.
        """
        docs = list(self._get_collection_ref(flow_id).limit(1).stream())
        if not docs:
            return None

        return {
            "flow_id": flow_id,
            "description": f"Banco de dados: {flow_id}",
            "source": "firebase",
        }

    def get_all_flows(self) -> Dict[str, Dict]:
        """
        Lista todas as coleções de nível raiz como flows.
        Cada coleção = um flow/database.
        """
        flows = {}
        for col_ref in self.db.collections():
            flow_id = col_ref.id
            flows[flow_id] = {
                "flow_id": flow_id,
                "description": f"Banco de dados: {flow_id}",
                "source": "firebase",
            }
        return flows

    def get_table(self, flow_id: str, table_name: str) -> Optional[Dict]:
        """
        Retorna metadados de uma tabela.
        Aceita tanto o ID do documento (TABLE_SALESLT_PRODUCT)
        quanto o table_name interno (Product).
        """
        # Tentativa direta pelo ID do documento
        doc = self._get_collection_ref(flow_id).document(table_name).get()
        if doc.exists:
            return self._doc_to_table(flow_id, doc)

        # Fallback: varrer e comparar pelo table_name interno
        for doc in self._get_collection_ref(flow_id).stream():
            data = doc.to_dict() or {}
            internal_name = data.get("table_definition", {}).get("table_name", "")
            if internal_name.lower() == table_name.lower():
                return self._doc_to_table(flow_id, doc)

        return None

    def get_tables_by_flow(self, flow_id: str) -> List[Dict]:
        """Retorna todas as tabelas (documentos) de um flow (coleção)"""
        tables = []
        for doc in self._get_collection_ref(flow_id).stream():
            tables.append(self._doc_to_table(flow_id, doc))
        return tables

    def get_ddl(self, flow_id: str, table_name: str) -> Optional[Dict]:
        """Retorna DDL de uma tabela no formato esperado pelo IntentAgent."""
        # Tentativa direta pelo ID do documento
        doc = self._get_collection_ref(flow_id).document(table_name).get()

        # Fallback: varrer pelo table_name interno
        if not doc.exists:
            for d in self._get_collection_ref(flow_id).stream():
                raw = (d.to_dict() or {}).get("table_definition", {})
                if raw.get("table_name", "").lower() == table_name.lower():
                    doc = d
                    break
            else:
                return None

        data = doc.to_dict() or {}
        table_def = data.get("table_definition", {})

        raw_columns = table_def.get("columns", [])
        columns = self._normalize_columns(raw_columns)

        # Primary keys via constraints
        constraints = table_def.get("constraints", {})
        pk_data = constraints.get("primary_key", {})
        if isinstance(pk_data, dict):
            primary_key = pk_data.get("columns", [])
        elif isinstance(pk_data, list):
            primary_key = pk_data
        else:
            primary_key = [col["name"] for col in columns if col.get("is_key_field")]

        # Foreign keys
        raw_fks = constraints.get("foreign_keys", [])
        foreign_keys = []
        for fk in (raw_fks if isinstance(raw_fks, list) else raw_fks.values()):
            from_cols = fk.get("from_columns", [])
            to_cols   = fk.get("to_columns", [])
            foreign_keys.append({
                "name": fk.get("name", ""),
                "column": from_cols[0] if from_cols else "",
                "references": {
                    "table": fk.get("to_table", ""),
                    "column": to_cols[0] if to_cols else "",
                },
            })

        return {
            "flow_id": flow_id,
            "table_name": table_def.get("table_name", table_name),
            "schema": table_def.get("schema", ""),
            "columns": [
                {
                    "name": col["name"],
                    "type": col["type"],
                    "nullable": col["nullable"],
                }
                for col in columns
            ],
            "constraints": {
                "primary_key": primary_key,
                "foreign_keys": foreign_keys,
            },
        }


class HybridFirebaseRepository:
    """
    Repositório híbrido: tenta Firestore primeiro, depois fallback local.
    """

    def __init__(self, firebase_repo: FirestoreFirebaseRepository,
                 local_repo):
        self.firebase = firebase_repo
        self.local = local_repo
        print("🔀 Repositório Híbrido: Firebase Firestore + Local")

    def get_flow(self, flow_id: str) -> Optional[Dict]:
        return self.firebase.get_flow(flow_id) or self.local.get_flow(flow_id)

    def get_all_flows(self) -> Dict[str, Dict]:
        flows = self.local.get_all_flows().copy()
        flows.update(self.firebase.get_all_flows())
        return flows

    def get_table(self, flow_id: str, table_name: str) -> Optional[Dict]:
        return self.firebase.get_table(flow_id, table_name) or self.local.get_table(flow_id, table_name)

    def get_tables_by_flow(self, flow_id: str) -> List[Dict]:
        tables = self.local.get_tables_by_flow(flow_id)
        tables.extend(self.firebase.get_tables_by_flow(flow_id))
        return tables

    def get_ddl(self, flow_id: str, table_name: str) -> Optional[Dict]:
        return self.firebase.get_ddl(flow_id, table_name) or self.local.get_ddl(flow_id, table_name)
