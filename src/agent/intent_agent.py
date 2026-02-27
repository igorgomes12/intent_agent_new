"""
Intent Agent
============
Agente que processa intenções recebidas de um orquestrador externo.

Suporta dois modos:
  - process()       : flow_id + table_name já conhecidos (modo legado)
  - scan_and_process(): varre todas as tabelas do flow e identifica a melhor
"""

from typing import Dict, Optional, List
from ..models.intent import IntentObject, ProcessStatus
from ..strategies.base import IntentStrategy
from ..strategies.local_strategy import LocalStrategy
from ..strategies.ai_strategy import AIStrategy
from ..services.validator import ValidationService


class IntentAgent:
    """Agente de Intenção - Processa dados recebidos do orquestrador"""

    def __init__(self,
                 repository,
                 local_strategy: LocalStrategy,
                 ai_strategy: AIStrategy,
                 gemini_threshold: float = 0.5):
        self.repo = repository
        self.local_strategy = local_strategy
        self.ai_strategy = ai_strategy
        self.gemini_threshold = gemini_threshold
        self.validator = ValidationService(repository)

    # ------------------------------------------------------------------
    # Modo varredura: identifica tabela automaticamente
    # ------------------------------------------------------------------

    def scan_and_process(self, user_prompt: str, flow_id: str) -> IntentObject:
        """
        Varre todas as tabelas do flow e resolve tabela + filtros em UMA única
        chamada ao Gemini.
        """
        print(f"\n🔍 Varrendo tabelas do flow: {flow_id}")

        flow_data = self.repo.get_flow(flow_id)
        if not flow_data:
            print(f"❌ Flow '{flow_id}' não encontrado")
            return self._create_error_intent(user_prompt, flow_id, "", [])

        all_tables = self.repo.get_tables_by_flow(flow_id)
        if not all_tables:
            print(f"❌ Nenhuma tabela encontrada no flow '{flow_id}'")
            return self._create_error_intent(user_prompt, flow_id, "", [])

        print(f"   {len(all_tables)} tabela(s) encontrada(s)")

        # Catálogo completo com colunas para o Gemini decidir tudo de uma vez
        catalog = self._build_catalog(all_tables)

        # UMA única chamada: seleciona tabela + extrai filtros
        # Injeta dicionário de valores se disponível
        dictionary_service = getattr(self.ai_strategy, '_dictionary_service', None)
        dictionary_block = dictionary_service.build_context_block() if dictionary_service else ""

        result = self.ai_strategy.ai_service.infer_scan(
            user_prompt, flow_id, catalog, dictionary_block
        )

        if not result:
            print("❌ AI não retornou resultado")
            return self._create_error_intent(user_prompt, flow_id, "", [])

        selected_table_name = result.get("selected_table", "").strip()
        print(f"✅ Tabela selecionada: {selected_table_name} "
              f"(confidence: {result.get('confidence', 0):.2f})")
        print(f"   💬 {result.get('reasoning', '')}")

        if not selected_table_name:
            return self._create_error_intent(user_prompt, flow_id, "", [])

        # Validar que a tabela existe
        # Aceita nome simples (TbProposta) ou qualificado (DBCRED.dbo.TbProposta)
        valid_names = [t["table"] for t in catalog]
        # Normaliza: se vier qualificado, pega a última parte
        if selected_table_name not in valid_names:
            last_part = selected_table_name.split(".")[-1]
            if last_part in valid_names:
                print(f"   🔄 Normalizado: {selected_table_name} → {last_part}")
                selected_table_name = last_part
            else:
                # Tenta match parcial
                match = next((n for n in valid_names
                              if selected_table_name.lower() in n.lower()
                              or n.lower() in selected_table_name.lower()), None)
                if match:
                    print(f"   🔄 Match parcial: {selected_table_name} → {match}")
                    selected_table_name = match
                else:
                    print(f"❌ Tabela '{selected_table_name}' não encontrada no catálogo")
                    return self._create_error_intent(user_prompt, flow_id, "", [])

        # Carregar DDL da tabela selecionada
        ddl_data = self.repo.get_ddl(flow_id, selected_table_name)
        if not ddl_data:
            raise ValueError(f"DDL não encontrado para {flow_id}.{selected_table_name}")

        print(f"✅ DDL carregado: {ddl_data.get('schema', '') or 'sem schema'}"
              f".{selected_table_name} ({len(ddl_data.get('columns', []))} colunas)")

        # Montar IntentObject com os filtros já retornados pelo Gemini
        return self._build_intent_from_scan(
            user_prompt, flow_id, selected_table_name, ddl_data, result
        )

    def _build_intent_from_scan(self, user_prompt: str, flow_id: str,
                                 table_name: str, ddl_data: Dict,
                                 ai_result: Dict) -> IntentObject:
        """Monta IntentObject a partir do resultado unificado do Gemini."""
        import hashlib
        from datetime import datetime
        from ..models.intent import FilterCondition, DDLReference

        all_warnings = []

        # Converter filtros
        raw_filters = ai_result.get("filters", [])
        filters = [
            FilterCondition(
                column=f["column"],
                operator=f["operator"],
                value=f["value"],
                nl_term=f.get("nl_term", ""),
                resolved_via="gemini_scan",
                confidence=f.get("confidence", 0.8),
            )
            for f in raw_filters
        ]

        # Validar colunas (aceita colunas de tabelas relacionadas)
        validated_filters, col_warnings = self.validator.validate_columns(
            filters, ddl_data.get("columns", []), main_table=table_name
        )
        all_warnings.extend(col_warnings)

        schema = ddl_data.get("schema", "")
        ddl_hash = hashlib.sha256(str(ddl_data).encode()).hexdigest()[:16]

        ddl_ref = DDLReference(
            flow_id=flow_id,
            table_name=table_name,
            schema=schema,
            ddl_hash=ddl_hash,
            columns_available=ddl_data.get("columns", []),
            constraints=ddl_data.get("constraints", {}),
            validated_at=datetime.now().isoformat(),
        )

        status, confidence = self.validator.calculate_status_and_confidence(
            all_warnings, ai_result.get("confidence_score", ai_result.get("confidence", 0.8))
        )

        if all_warnings:
            print(f"\n⚠️  {len(all_warnings)} warning(s)")
            for w in all_warnings:
                print(f"   [{w.level.value}] {w.message}")

        print(f"\n✅ Status: {status.value} | Confidence: {confidence:.2f}")

        return IntentObject(
            flow_id=flow_id,
            table_name=table_name,
            intent_type="query",
            filters=validated_filters,
            select_columns=ai_result.get("select_columns", []),
            joins=[],
            order_by=ai_result.get("order_by", []),
            limit=ai_result.get("limit", 0),
            confidence_score=confidence,
            metadata={"schema": schema, "gemini_reasoning": ai_result.get("reasoning", "")},
            ddl_reference=ddl_ref,
            sources_consulted={"ddl": True, "gemini_scan": True},
            original_prompt=user_prompt,
            created_at=datetime.now().isoformat(),
            status=status,
            warnings=all_warnings,
        )

    def _build_catalog(self, tables: List[Dict]) -> List[Dict]:
        """
        Monta catálogo completo: nome + schema + database + colunas + FKs.
        full_ref usa 4 partes quando database disponível: database.schema.tabela.coluna
        Passado ao Gemini para que ele decida tabela e filtros de uma vez.
        """
        catalog = []
        for t in tables:
            profile = t.get("table_profile", {})
            table_name = profile.get("table_name", "")
            schema = profile.get("schema", "")
            database = profile.get("database", "")

            # Monta full_ref com database se disponível
            def make_full_ref(col_name: str) -> str:
                if database and schema:
                    return f"{database}.{schema}.{table_name}.{col_name}"
                elif schema:
                    return f"{schema}.{table_name}.{col_name}"
                return f"{table_name}.{col_name}"

            columns = [
                {
                    "name": c.get("name", ""),
                    "type": c.get("type", ""),
                    "full_ref": make_full_ref(c.get("name", "")),
                    "is_key_field": c.get("is_key_field", False),
                }
                for c in t.get("columns_dictionary", [])
            ]

            # FKs: normaliza foreign_key_hints para formato consistente
            table_def = t.get("original_data", {}).get("table_definition", {})
            constraints = table_def.get("constraints", {})
            raw_fks = constraints.get("foreign_key_hints", constraints.get("foreign_keys", []))
            
            # Normaliza FKs para formato legível no prompt
            # foreign_key_hints: from_table é a tabela referenciada, to_table é a tabela atual
            normalized_fks = []
            for fk in raw_fks:
                from_table = fk.get("from_table", "")
                to_table = fk.get("to_table", "")
                column = fk.get("column", "")
                
                # Formato: tabela_atual.coluna → tabela_referenciada.coluna
                normalized_fks.append({
                    "from_table": to_table,  # Tabela atual (onde está a FK)
                    "to_table": from_table,  # Tabela referenciada
                    "column": column,
                    "relationship_name": fk.get("relationship_name", "")
                })

            catalog.append({
                "table": table_name,
                "schema": schema,
                "database": database,
                "description": profile.get("description", ""),
                "columns": columns,
                "foreign_keys": normalized_fks,
            })
        return catalog

    # ------------------------------------------------------------------
    # Modo legado: flow_id + table_name já conhecidos
    # ------------------------------------------------------------------

    def process(self,
                user_prompt: str,
                flow_id: str,
                table_name: str,
                flow_score: Optional[float] = None) -> IntentObject:
        """
        Processa intenção com flow e tabela já identificados.
        """
        print(f"\n🔍 Processando intenção recebida")
        print(f"   Prompt: \"{user_prompt}\"")
        print(f"   Flow: {flow_id}")
        print(f"   Tabela: {table_name}")

        all_warnings = []

        # STEP 1: Validação crítica
        print(f"\n🔒 Validando flow e tabela...")
        exists, validation_warnings = self.validator.validate_flow_and_table(flow_id, table_name)
        all_warnings.extend(validation_warnings)

        if not exists:
            print(f"❌ Validação crítica falhou")
            return self._create_error_intent(user_prompt, flow_id, table_name, all_warnings)

        print(f"✅ Flow e tabela validados")

        # STEP 2: Carregar DDL
        ddl_data = self.repo.get_ddl(flow_id, table_name)
        if not ddl_data:
            raise ValueError(f"❌ DDL não encontrado para {flow_id}.{table_name}")

        print(f"✅ DDL carregado: {ddl_data.get('schema', '') or 'sem schema'}.{table_name} "
              f"({len(ddl_data.get('columns', []))} colunas)")

        # STEP 3: Mocks de compatibilidade com estratégias
        flow_match = {
            "flow_id": flow_id,
            "_match_score": flow_score if flow_score is not None else 10,
        }
        table_match = {"table_profile": {"table_name": table_name}}

        # STEP 4: Decidir estratégia
        strategy = self._select_strategy(
            flow_match["_match_score"], user_prompt, flow_id, table_name
        )

        # STEP 5: Construir intent
        intent = strategy.build_intent(user_prompt, flow_match, table_match, ddl_data)

        # STEP 6: Validar colunas
        print(f"\n🔍 Validando colunas...")
        validated_filters, column_warnings = self.validator.validate_columns(
            intent.filters, ddl_data.get("columns", []), main_table=table_name
        )
        all_warnings.extend(column_warnings)
        intent.filters = validated_filters

        if intent.select_columns:
            validated_select, select_warnings = self.validator.validate_select_columns(
                intent.select_columns, ddl_data.get("columns", [])
            )
            all_warnings.extend(select_warnings)
            intent.select_columns = validated_select

        # STEP 7: Status e confidence final
        status, adjusted_confidence = self.validator.calculate_status_and_confidence(
            all_warnings, intent.confidence_score
        )
        intent.status = status
        intent.confidence_score = adjusted_confidence
        intent.warnings = all_warnings

        if all_warnings:
            print(f"\n⚠️  {len(all_warnings)} warning(s)")
            for w in all_warnings:
                print(f"   [{w.level.value}] {w.message}")

        print(f"\n✅ Status: {status.value} | Confidence: {adjusted_confidence:.2f}")
        return intent

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _select_strategy(self, flow_score: int, user_prompt: str,
                         flow_id: str, table_name: str) -> IntentStrategy:
        """Decide qual estratégia usar"""
        from ..services.filter_extractor import FilterExtractor
        filter_extractor = FilterExtractor(self.repo)
        columns = filter_extractor.get_candidate_columns(flow_id, table_name, user_prompt)
        use_gemini = (flow_score < self.gemini_threshold * 10) or (len(columns) < 2)

        if use_gemini:
            print(f"🤖 Usando Gemini AI")
            return self.ai_strategy
        else:
            print(f"📊 Usando busca local (score: {flow_score})")
            return self.local_strategy

    def _create_error_intent(self, user_prompt: str, flow_id: str,
                             table_name: str, warnings: list) -> IntentObject:
        from datetime import datetime
        from ..models.intent import DDLReference

        return IntentObject(
            flow_id=flow_id,
            table_name=table_name,
            intent_type="error",
            filters=[],
            select_columns=[],
            joins=[],
            order_by=[],
            limit=0,
            confidence_score=0.0,
            metadata={"error": "validation_failed"},
            ddl_reference=DDLReference(
                flow_id=flow_id,
                table_name=table_name,
                schema="",
                ddl_hash="",
                columns_available=[],
                constraints={},
                validated_at=datetime.now().isoformat(),
            ),
            sources_consulted={},
            original_prompt=user_prompt,
            created_at=datetime.now().isoformat(),
            status=ProcessStatus.ERROR,
            warnings=warnings,
        )
