"""
Prompt Builder
==============
Prompts compartilhados entre os serviços de AI.
"""


def build_scan_prompt(user_query: str, flow_id: str, catalog: list,
                      dictionary_block: str = "") -> str:
    """
    Prompt unificado: seleciona tabela + extrai filtros em uma única chamada.
    
    Otimizado para reduzir tamanho e evitar truncamento:
    - Lista apenas tabelas principais (não todas as colunas)
    - Colunas enviadas apenas para tabelas relevantes
    """
    # Tabelas: nome simples para selected_table
    tables_lines = []
    table_names = []
    for t in catalog:
        db = t.get("database", "")
        schema = t.get("schema", "")
        tname = t.get("table", "")
        desc = t.get("description", "")
        table_names.append(tname)
        
        # Identificador legível da tabela
        if db and schema:
            label = f"{db}.{schema}.{tname}"
        elif schema:
            label = f"{schema}.{tname}"
        else:
            label = tname
        tables_lines.append(f"  - {tname} [{label}]: {desc}")

    # Colunas: apenas as principais (key fields + campos com description)
    # Para reduzir tamanho do prompt
    important_cols = []
    for t in catalog:
        tname = t.get("table", "")
        for c in t.get("columns", []):
            # Inclui apenas colunas chave ou com palavras-chave importantes no nome
            col_name = c.get("name", "")
            col_name_lower = col_name.lower()
            
            # Prefixos importantes (case-insensitive)
            important_prefixes = ['nu', 'cd', 'dt', 'st', 'tp', 'vr', 'nm', 'ds', 'fl', 'pc', 'qt', 'aa']
            
            if (c.get("is_key_field") or 
                any(col_name_lower.startswith(prefix) for prefix in important_prefixes)):
                important_cols.append(f"  - {c['full_ref']} ({c['type']})")
    
    # Limita a 150 colunas mais importantes (aumentado de 100)
    cols_list = "\n".join(important_cols[:150]) or "  (nenhuma)"

    # FKs: formato compacto
    fk_lines = []
    for t in catalog:
        for fk in t.get("foreign_keys", []):
            if "from_table" in fk and "to_table" in fk:
                col = fk.get("column", "")
                from_t = fk.get("from_table", "")
                to_t = fk.get("to_table", "")
                fk_lines.append(f"  - {from_t}.{col} → {to_t}.{col}")
    
    fk_list = "\n".join(fk_lines[:50]) or "  (nenhuma)"  # Limita a 50 FKs

    tables_summary = "\n".join(tables_lines)
    table_names_str = ", ".join(table_names)

    return f"""Você é um assistente que analisa consultas em linguagem natural e extrai filtros estruturados.

USER QUERY: "{user_query}"

BANCO: {flow_id}

TABELAS (use nome simples em "selected_table"):
{tables_summary}

NOMES VÁLIDOS: {table_names_str}

COLUNAS PRINCIPAIS (use full_ref exato em "column"):
{cols_list}

RELACIONAMENTOS:
{fk_list}

{dictionary_block}

TAREFA:
1. Identifique a TABELA PRINCIPAL
2. Extraia FILTROS usando colunas de qualquer tabela relacionada
3. Liste colunas de retorno em "select_columns"

CRÍTICO - TABELA PRINCIPAL:
- Para QUALQUER query sobre propostas, veículos, produtos, CDC:
  * "selected_table" DEVE SER "TbProduto"
  * NUNCA use "TbProposta" como selected_table
  * NUNCA use "TbSubProduto" como selected_table
  * NUNCA use "TbModalidadeProduto" como selected_table
- A query SEMPRE começa: FROM DBCOR.dbo.TbProduto
- Outras tabelas são acessadas via JOIN

ESTRUTURA BASE OBRIGATÓRIA (4 TABELAS ESSENCIAIS):
Toda query DEVE incluir estas 4 tabelas na estrutura base:
1. DBCOR.dbo.TbProduto (tabela principal - FROM)
2. DBCOR.dbo.TbSubProduto (INNER JOIN)
3. DBCOR.dbo.TbModalidadeProduto (INNER JOIN)
4. DBCRED.dbo.TbProposta (INNER JOIN)

Estrutura SQL obrigatória:
FROM DBCOR.dbo.TbProduto
INNER JOIN DBCOR.dbo.TbSubProduto 
    ON DBCOR.dbo.TbProduto.CdProduto = DBCOR.dbo.TbSubProduto.CdProduto
INNER JOIN DBCOR.dbo.TbModalidadeProduto 
    ON DBCOR.dbo.TbSubProduto.CdModalidadeProduto = DBCOR.dbo.TbModalidadeProduto.CdModalidadeProduto
    AND DBCOR.dbo.TbProduto.CdProduto = DBCOR.dbo.TbModalidadeProduto.CdProduto
INNER JOIN DBCRED.dbo.TbProposta 
    ON DBCOR.dbo.TbSubProduto.CdSubProduto = DBCRED.dbo.TbProposta.CdSubProduto

REGRAS:
- "selected_table": nome simples (ex: TbProposta)
- "column": full_ref exato (ex: DBCRED.dbo.TbProposta.StProposta)
- "value": APENAS literal — NUNCA subselect
- Operadores: =, >, <, >=, <=, IN, LIKE, BETWEEN, IS NOT NULL
- Datas: DATEADD(day, -N, GETDATE()) ou formato 'yyyy-MM-dd HH:mm:ss.SSS' (ex: '2020-02-01 00:00:00.000')
- IN: valores separados por vírgula (ex: "12, 13")
- "select_columns": full_ref de qualquer tabela
- Colunas de data começam com "Dt" (ex: DtVencimento, DtEntrada, DtNascimento)

REGRA DE NEGÓCIO:
- Tipo de pessoa: SEMPRE use DBCOR.dbo.TbPessoa.TpPessoa (F=Física, J=Jurídica)
- NUNCA use DBCRED.dbo.TbPessoaSobAnalise.TpPessoa
- Return fields: SEMPRE retorne APENAS DBCRED.dbo.TbProposta.NuProposta em "select_columns"

ESTRUTURA DE QUERY (CRÍTICO - SEMPRE IGUAL):
FROM DBCOR.dbo.TbProduto
INNER JOIN DBCOR.dbo.TbSubProduto 
    ON DBCOR.dbo.TbProduto.CdProduto = DBCOR.dbo.TbSubProduto.CdProduto
INNER JOIN DBCOR.dbo.TbModalidadeProduto 
    ON DBCOR.dbo.TbSubProduto.CdModalidadeProduto = DBCOR.dbo.TbModalidadeProduto.CdModalidadeProduto
    AND DBCOR.dbo.TbProduto.CdProduto = DBCOR.dbo.TbModalidadeProduto.CdProduto
INNER JOIN DBCRED.dbo.TbProposta 
    ON DBCOR.dbo.TbSubProduto.CdSubProduto = DBCRED.dbo.TbProposta.CdSubProduto

JOINS OPCIONAIS (adicione conforme necessário):
- TbPessoa: INNER JOIN quando filtrar por tipo de pessoa (F/J)
  INNER JOIN DBCOR.dbo.TbPessoa ON DBCRED.dbo.TbProposta.CdPessoa = DBCOR.dbo.TbPessoa.CdPessoa

- TbPropostaSeguro: 
  * COM seguro: INNER JOIN DBCRED.dbo.TbPropostaSeguro ON DBCRED.dbo.TbProposta.NuProposta = DBCRED.dbo.TbPropostaSeguro.NuProposta
  * SEM seguro: LEFT JOIN + WHERE DBCRED.dbo.TbPropostaSeguro.NuProposta IS NULL

- TbPropostaGarantia:
  * COM garantia na proposta: INNER JOIN DBCRED.dbo.TbPropostaGarantia ON DBCRED.dbo.TbProposta.NuProposta = DBCRED.dbo.TbPropostaGarantia.NuProposta
  * SEM garantia na proposta: LEFT JOIN + WHERE DBCRED.dbo.TbPropostaGarantia.NuProposta IS NULL

- TbPropGarantiaMaqEquip: APENAS para impressão de contratos ou detalhes do veículo
  LEFT JOIN DBCRED.dbo.TbPropGarantiaMaqEquip ON DBCRED.dbo.TbProposta.NuProposta = DBCRED.dbo.TbPropGarantiaMaqEquip.NuProposta

MAPEAMENTO DE CÓDIGOS:
TbProduto.CdProduto:
  12 = CDC COM GARANTIA
  13 = CDC SEM GARANTIA

TbModalidadeProduto.CdModalidadeProduto:
  5 = MOTOS
  7 = VANS E MICRO-ÔNIBUS
  8 = VEÍCULOS LEVES
  9 = VEÍCULOS PESADOS

REGRAS DE FILTROS:
1. Tipo de veículo (leves, pesados, motos, vans):
   → Filtro: DBCOR.dbo.TbModalidadeProduto.CdModalidadeProduto = [5|7|8|9]

2. CDC com/sem garantia:
   → Filtro: DBCOR.dbo.TbProduto.CdProduto = [12|13]

3. Proposta COM garantia (garantia na proposta):
   → INNER JOIN TbPropostaGarantia (sem filtro WHERE adicional)

4. Proposta SEM garantia (sem garantia na proposta):
   → LEFT JOIN TbPropostaGarantia + WHERE DBCRED.dbo.TbPropostaGarantia.NuProposta IS NULL

5. Proposta COM seguro:
   → INNER JOIN TbPropostaSeguro (sem filtro WHERE adicional)

6. Proposta SEM seguro:
   → LEFT JOIN TbPropostaSeguro + WHERE DBCRED.dbo.TbPropostaSeguro.NuProposta IS NULL

IMPORTANTE: 
- "CDC com garantia" refere-se a TbProduto.CdProduto = 12
- "Proposta com garantia" refere-se a TbPropostaGarantia (JOIN + IS NOT NULL ou IS NULL)
- São conceitos DIFERENTES!

REGRAS DE FILTROS ESPECIAIS:
- Proposta COM garantia: adicione filtro "DBCRED.dbo.TbPropostaGarantia.NuProposta" com operador "IS NOT NULL"
- Proposta SEM garantia: adicione filtro "DBCRED.dbo.TbPropostaGarantia.NuProposta" com operador "IS NULL"
- Proposta COM seguro: adicione filtro "DBCRED.dbo.TbPropostaSeguro.NuProposta" com operador "IS NOT NULL"
- Proposta SEM seguro: adicione filtro "DBCRED.dbo.TbPropostaSeguro.NuProposta" com operador "IS NULL"
- Para IS NULL ou IS NOT NULL, deixe "value" vazio ("")

Retorne JSON válido (sem markdown):
{{
  "selected_table": "NomeSimples",
  "reasoning": "explicação",
  "confidence": 0.95,
  "filters": [
    {{
      "column": "DBCRED.dbo.TbProposta.StProposta",
      "operator": "IN",
      "value": "A, AP",
      "nl_term": "aprovadas",
      "confidence": 0.95
    }},
    {{
      "column": "DBCRED.dbo.TbPropostaGarantia.NuProposta",
      "operator": "IS NOT NULL",
      "value": "",
      "nl_term": "com garantia",
      "confidence": 0.95
    }}
  ],
  "select_columns": ["DBCRED.dbo.TbProposta.NuProposta"],
  "order_by": [],
  "limit": 0,
  "confidence_score": 0.95
}}"""
