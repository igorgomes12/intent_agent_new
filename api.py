"""
API REST para o Intent Agent
=============================
Endpoint POST /query para processar prompts em linguagem natural
e retornar JSON estruturado com filtros e DDL.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Optional
from dotenv import load_dotenv
import httpx

# Carregar .env ANTES de importar src
load_dotenv(override=True)

from src.factory import IntentAgentFactory
from src.config.settings import Settings

# Inicializar FastAPI
app = FastAPI(
    title="Intent Agent API",
    description="API para processar queries em linguagem natural e gerar estruturas SQL",
    version="1.0.0"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Modelos Pydantic
class QueryRequest(BaseModel):
    prompt: str

class QueryResponse(BaseModel):
    parameters: Dict
    ddl: List[Dict]
    generated_query: Optional[Dict] = None
    execution_result: Optional[Dict] = None

# Inicializar agente (singleton)
settings = Settings.from_env()
agent = IntentAgentFactory.create(settings)

# Flow ID fixo
FLOW_ID = "FLUXO_PROPOSTA_VEICULO_CRM"

# URLs das outras APIs
QUERY_GENERATOR_URL = "http://localhost:8000/query"
QUERY_EXECUTOR_URL = "http://localhost:8001/exec_single_query"

@app.post("/query", response_model=QueryResponse)
async def process_query(request: QueryRequest):
    """
    Processa um prompt em linguagem natural e retorna query estruturada
    
    Args:
        request: QueryRequest com prompt
        
    Returns:
        QueryResponse com parameters e ddl
    """
    try:
        # Processar prompt
        intent = agent.scan_and_process(
            user_prompt=request.prompt,
            flow_id=FLOW_ID,
        )
        
        # Extrair informações
        ddl = intent.ddl_reference
        schema = ddl.schema
        main_table = intent.table_name
        main_ddl_full = agent.repo.get_ddl(FLOW_ID, main_table)
        main_database = main_ddl_full.get("database", "") if main_ddl_full else ""
        
        # Função auxiliar para criar referências completas
        def make_ref(db, sch, tbl, col):
            if db and sch:
                return f"{db}.{sch}.{tbl}.{col}"
            elif sch:
                return f"{sch}.{tbl}.{col}"
            return f"{tbl}.{col}"
        
        # Montar filter_fields
        filter_fields = []
        for f in intent.filters:
            col = f.column
            val = f.value
            op = f.operator
            
            # Para IS NULL e IS NOT NULL, não precisa de valor
            if op.upper() in ["IS NULL", "IS NOT NULL"]:
                filter_fields.append({col: op})
            else:
                # Formatar valor
                if isinstance(val, str) and not val.replace('.','').replace('-','').isnumeric() and not val.upper().startswith('DATEADD'):
                    formatted_val = f"'{val}'"
                else:
                    formatted_val = str(val)
                filter_fields.append({col: f"{op} {formatted_val}"})
        
        # Montar return_fields - SEMPRE retornar apenas NuProposta
        return_fields = ["DBCRED.dbo.TbProposta.NuProposta"]

        # Garantir filtro obrigatório de DtEntradaProposta
        MANDATORY_FILTER_KEY = "DBCRED.dbo.TbProposta.DtEntradaProposta"
        MANDATORY_FILTER_VAL = "> '2020-01-01 00:00:00.000'"
        has_dt_filter = any(
            MANDATORY_FILTER_KEY in ff for ff in filter_fields
        )
        if not has_dt_filter:
            filter_fields.append({MANDATORY_FILTER_KEY: MANDATORY_FILTER_VAL})

        # Tabelas obrigatórias que sempre devem aparecer em tables_referenced
        MANDATORY_TABLES = [
            "DBCOR.dbo.TbProduto",
            "DBCOR.dbo.TbSubProduto",
            "DBCOR.dbo.TbModalidadeProduto",
            "DBCRED.dbo.TbProposta",
        ]

        # Derivar tabelas referenciadas
        tables_referenced = []
        seen = set()
        
        for ff in filter_fields:
            for key in ff:
                parts = key.split(".")
                if len(parts) >= 2:
                    table_ref = ".".join(parts[:-1])
                    if table_ref not in seen:
                        seen.add(table_ref)
                        tables_referenced.append(table_ref)
        
        for rf in return_fields:
            parts = rf.split(".")
            if len(parts) >= 2:
                table_ref = ".".join(parts[:-1])
                if table_ref not in seen:
                    seen.add(table_ref)
                    tables_referenced.append(table_ref)

        # Garantir que as 4 tabelas obrigatórias estejam sempre presentes
        for mandatory in MANDATORY_TABLES:
            if mandatory not in seen:
                seen.add(mandatory)
                tables_referenced.append(mandatory)

        # Coletar DDLs das tabelas necessárias
        tables_by_db: Dict[str, list] = {}
        
        def add_table_to_db(ddl_data: dict):
            db = ddl_data.get("database", "default")
            if db not in tables_by_db:
                tables_by_db[db] = []
            existing = [t["name"] for t in tables_by_db[db]]
            if ddl_data.get("table_name", "") not in existing:
                table_entry = {
                    "schema": ddl_data.get("schema", ""),
                    "name": ddl_data.get("table_name", ""),
                    "columns": ddl_data.get("columns", []),
                    "primaryKey": ddl_data.get("constraints", {}).get("primary_key", []),
                    "foreignKeys": ddl_data.get("constraints", {}).get("foreign_keys", [])
                }
                tables_by_db[db].append(table_entry)
        
        # Identificar tabelas necessárias
        tables_needed = set()
        for ff in filter_fields:
            for key in ff:
                parts = key.split(".")
                if len(parts) >= 2:
                    tables_needed.add(parts[-2].lower())
        
        for rf in return_fields:
            parts = rf.split(".")
            if len(parts) >= 2:
                tables_needed.add(parts[-2].lower())
        
        for tname_lower in tables_needed:
            t_ddl = agent.repo.get_ddl(FLOW_ID, tname_lower)
            if not t_ddl:
                for t in agent.repo.get_tables_by_flow(FLOW_ID):
                    if t["table_profile"]["table_name"].lower() == tname_lower:
                        t_ddl = agent.repo.get_ddl(FLOW_ID, t["table_profile"]["table_name"])
                        break
            if t_ddl:
                add_table_to_db(t_ddl)
        
        ddl_output = [
            {"database": db, "tipo": "SYBASE", "tables": tbls}
            for db, tbls in tables_by_db.items()
        ]
        
        if not ddl_output:
            ddl_output = [{"database": main_database or "default", "tipo": "SYBASE", "tables": []}]
        
        # Montar resposta
        response = {
            "parameters": {
                "filter_fields": filter_fields,
                "tables": tables_referenced,
                "return_fields": return_fields
            },
            "ddl": ddl_output
        }
        
        # Chamar API geradora de query (porta 8000)
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                query_gen_response = await client.post(
                    QUERY_GENERATOR_URL,
                    json=response
                )
                if query_gen_response.status_code == 200:
                    query_result = query_gen_response.json()
                    response["generated_query"] = query_result
                    
                    # Chamar API executora de query (porta 8001)
                    try:
                        exec_response = await client.post(
                            QUERY_EXECUTOR_URL,
                            json=query_result
                        )
                        if exec_response.status_code == 200:
                            response["execution_result"] = exec_response.json()
                        else:
                            response["execution_result"] = {
                                "error": f"Erro ao executar query: {exec_response.status_code}",
                                "detail": exec_response.text
                            }
                    except Exception as e:
                        response["execution_result"] = {
                            "error": f"Erro ao chamar API executora: {str(e)}"
                        }
                else:
                    response["generated_query"] = {
                        "error": f"Erro ao gerar query: {query_gen_response.status_code}",
                        "detail": query_gen_response.text
                    }
        except Exception as e:
            response["generated_query"] = {
                "error": f"Erro ao chamar API geradora: {str(e)}"
            }
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar query: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
