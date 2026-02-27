"""
Query Output Models
===================
Formato de saída para o agente de query (com PKs, FKs e schema completo).
"""

from typing import List, Dict, Any
from .intent import IntentObject


def convert_intent_to_query_format(intent: IntentObject,
                                   repository=None) -> Dict:
    """
    Converte IntentObject para o contrato esperado pelo agente de query.

    Formato de saída:
    {
      "parameters": {
        "filter_fields": [{"schema.table.col": "op value"}, ...],
        "return_fields":  ["schema.table.col", ...]
      },
      "ddl": {
        "database": "...",
        "tipo": "SYBASE",
        "tables": [{ schema, name, columns, primaryKey?, foreignKeys? }, ...]
      }
    }

    Args:
        intent:     IntentObject produzido pelo agente
        repository: FirestoreFirebaseRepository (opcional).
                    Se fornecido, busca tabelas relacionadas via
                    relationships_hints para compor o DDL completo.
    """
    schema = intent.ddl_reference.schema
    table  = intent.table_name

    # ── filter_fields ────────────────────────────────────────────
    filter_fields = [
        {f"{schema}.{table}.{f.column}": f"{f.operator} {f.value}"}
        for f in intent.filters
    ]

    # ── return_fields ────────────────────────────────────────────
    blocked = intent.metadata.get('blocked_columns', [])
    if intent.select_columns:
        return_fields = [f"{schema}.{table}.{col}" for col in intent.select_columns]
    else:
        return_fields = [
            f"{schema}.{table}.{col['name']}"
            for col in intent.ddl_reference.columns_available
            if col['name'] not in blocked
        ]

    # ── DDL: tabela principal ─────────────────────────────────────
    main_table = _build_table_entry(
        schema=schema,
        name=table,
        columns=intent.ddl_reference.columns_available,
        constraints=intent.ddl_reference.constraints,
    )
    tables = [main_table]

    # ── DDL: tabelas relacionadas (via Firestore) ─────────────────
    if repository is not None:
        related = _fetch_related_tables(intent, repository)
        tables.extend(related)

    ddl_output = {
        "database": intent.metadata.get('database_name', 'default'),
        "tipo":     intent.metadata.get('database_type', 'SYBASE'),
        "tables":   tables,
    }

    return {
        "parameters": {
            "filter_fields": filter_fields,
            "return_fields": return_fields,
        },
        "ddl": ddl_output,
    }


# ── helpers ──────────────────────────────────────────────────────

def _build_table_entry(schema: str, name: str,
                       columns: List[Dict], constraints: Dict) -> Dict:
    """Monta um item de tabela no formato do contrato."""
    entry: Dict[str, Any] = {
        "schema":  schema,
        "name":    name,
        "columns": [
            {
                "name":     col["name"],
                "type":     col.get("type", "string"),
                "nullable": col.get("nullable", True),
            }
            for col in columns
        ],
    }

    pk = constraints.get("primary_key", [])
    if pk:
        entry["primaryKey"] = pk

    fks = constraints.get("foreign_keys", [])
    if fks:
        entry["foreignKeys"] = [
            {
                "name":   fk.get("name", f"FK_{fk['column']}"),
                "column": fk["column"],
                "references": {
                    "table":  fk["references"]["table"],
                    "column": fk["references"]["column"],
                },
            }
            for fk in fks
        ]

    return entry


def _fetch_related_tables(intent: IntentObject, repository) -> List[Dict]:
    """
    Busca tabelas relacionadas usando relationships_hints do Firestore.

    Estratégia de resolução do DDL de cada tabela relacionada:
      1. Busca no próprio flow (get_ddl com o mesmo flow_id)
      2. Se não encontrar, varre todos os outros flows procurando
         um documento cuja table_definition.table_name bata
      3. Se ainda não encontrar, monta entrada mínima com as
         colunas de join conhecidas pelo hints
    """
    related: List[Dict] = []
    seen = {intent.table_name}

    try:
        doc = repository.fluxos_ref.document(intent.flow_id).get()
        if not doc.exists:
            return related

        data = doc.to_dict()
        hints = (
            data.get("ai_and_rag_support", {})
                .get("relationships_hints", {})
                .get("outgoing", [])
        )

        # Cache de todos os flows para evitar múltiplas chamadas
        all_flow_docs = None

        for hint in hints:
            to_table = hint.get("to_table")
            if not to_table or to_table in seen:
                continue
            seen.add(to_table)

            # 1. Tentar no próprio flow
            related_ddl = repository.get_ddl(intent.flow_id, to_table)

            # 2. Buscar em outros flows
            if not related_ddl:
                if all_flow_docs is None:
                    all_flow_docs = list(repository.fluxos_ref.stream())

                for other_doc in all_flow_docs:
                    if other_doc.id == intent.flow_id:
                        continue
                    other_data = other_doc.to_dict()
                    other_table = other_data.get("table_definition", {}).get("table_name")
                    if other_table == to_table:
                        related_ddl = repository.get_ddl(other_doc.id, to_table)
                        if related_ddl:
                            print(f"   📎 DDL de {to_table} encontrado no flow: {other_doc.id}")
                            break

            # 3. Montar entrada
            if related_ddl:
                entry = _build_table_entry(
                    schema=related_ddl.get("schema", intent.ddl_reference.schema),
                    name=to_table,
                    columns=related_ddl.get("columns", []),
                    constraints=related_ddl.get("constraints", {}),
                )
            else:
                # Entrada mínima com colunas de join conhecidas
                join_cols = [
                    j.get("right", "").split(".")[-1]
                    for j in hint.get("join", [])
                    if j.get("right")
                ]
                print(f"   ⚠️  DDL de {to_table} não encontrado em nenhum flow — usando colunas de join")
                entry = {
                    "schema":  intent.ddl_reference.schema,
                    "name":    to_table,
                    "columns": [
                        {"name": col, "type": "string", "nullable": True}
                        for col in join_cols
                    ],
                }

            related.append(entry)

    except Exception as e:
        print(f"⚠️  Não foi possível buscar tabelas relacionadas: {e}")

    return related
