"""
Dictionary Service
==================
Carrega dicionários locais de mapeamento de valores e formata
o bloco de contexto que é injetado no prompt do Gemini.

Estrutura esperada em cada arquivo JSON:
{
  "table": "NomeDaTabela",
  "fields": {
    "NomeDoCampo": {
      "description": "Descrição legível",
      "values": {
        "termo em português": "CODIGO_NO_BANCO",
        ...
      }
    }
  }
}
"""

import json
import os
from typing import Dict


class DictionaryService:
    """Carrega e formata dicionários locais de valores."""

    def __init__(self, dictionaries_path: str = None):
        if dictionaries_path is None:
            dictionaries_path = os.path.join(
                os.path.dirname(__file__)
            )
        self._path = dictionaries_path
        self._data: Dict[str, Dict] = {}
        self._load()

    def _load(self):
        """Carrega todos os arquivos .json da pasta de dicionários."""
        for filename in os.listdir(self._path):
            if not filename.endswith(".json"):
                continue
            filepath = os.path.join(self._path, filename)
            try:
                with open(filepath, encoding="utf-8") as f:
                    data = json.load(f)
                table = data.get("table", filename.replace(".json", ""))
                self._data[table] = data
            except Exception as e:
                print(f"⚠️  Dicionário '{filename}' não carregado: {e}")

        if self._data:
            tables = list(self._data.keys())
            print(f"📖 Dicionários carregados: {tables}")

    def build_context_block(self) -> str:
        """
        Retorna o bloco de texto que será injetado no prompt do Gemini.
        Formato legível para o modelo entender os mapeamentos.
        """
        if not self._data:
            return ""

        lines = ["DICIONÁRIO DE VALORES (use para resolver termos do usuário):"]

        for table, data in self._data.items():
            for field, field_data in data.get("fields", {}).items():
                desc = field_data.get("description", field)
                lines.append(f"\n  {table}.{field} ({desc}):")
                for term, code in field_data.get("values", {}).items():
                    lines.append(f'    "{term}" → "{code}"')
            
            # Adiciona informações sobre tabelas relacionadas
            related = data.get("related_tables", {})
            if related:
                lines.append(f"\n  {table} - Tabelas Relacionadas:")
                for rel_table, rel_info in related.items():
                    relationship = rel_info.get("relationship", "")
                    rel_desc = rel_info.get("description", "")
                    lines.append(f'    → {rel_table}: {relationship}')
                    if rel_desc:
                        lines.append(f'      ({rel_desc})')

        lines.append(
            "\n  Quando o usuário usar um desses termos, use o código correspondente "
            "como valor do filtro."
        )
        lines.append(
            "\n  IMPORTANTE: Quando identificar termos de múltiplas tabelas relacionadas, "
            "inclua filtros para TODAS as tabelas mencionadas."
        )

        return "\n".join(lines)

    def is_empty(self) -> bool:
        return len(self._data) == 0
