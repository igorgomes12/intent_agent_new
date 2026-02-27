# Agente de Intenção

Este projeto lê uma pergunta em português e devolve um JSON estruturado com os filtros e o DDL necessários para outro agente gerar uma query SQL.

**Exemplo:**
> "liste todos os produtos da categoria Mountain Bikes"

Vira:
```json
{
  "parameters": {
    "filter_fields": [
      { "SalesLT.ProductCategory.Name": "= 'Mountain Bikes'" }
    ],
    "return_fields": ["SalesLT.Product.ProductID", "SalesLT.Product.Name", "..."]
  },
  "ddl": { "..." }
}
```

---

## O que você vai precisar

- Python 3.10 ou superior
- Uma conta no [Google AI Studio](https://aistudio.google.com/) para obter uma API Key gratuita
- Acesso ao projeto no Google Cloud (Firestore já configurado)
- Os arquivos `key.json` e `firestore-key.json` — peça para quem configurou o projeto

---

## Passo a passo para rodar

### 1. Instale o Python

Baixe em https://www.python.org/downloads/ e marque a opção **"Add Python to PATH"** durante a instalação.

### 2. Baixe o projeto

```bash
git clone <url-do-repositorio>
cd <nome-da-pasta>
```

### 3. Crie um ambiente virtual

```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Linux/macOS
```

> Você saberá que funcionou quando aparecer `(venv)` no início do terminal.

### 4. Instale as dependências

```bash
pip install -r requirements.txt
```

### 5. Configure o arquivo `.env`

Copie o arquivo de exemplo:

```bash
cp .env.example .env
```

Abra o `.env` e preencha:

```env
GCP_PROJECT_ID=steady-computer-487217-p6
GCP_LOCATION=us-central1
GOOGLE_APPLICATION_CREDENTIALS=./key.json

USE_FIRESTORE=true
FIRESTORE_PROJECT_ID=steady-computer-487217-p6
FIRESTORE_CREDENTIALS=./firestore-key.json
FIRESTORE_DATABASE=(default)

GOOGLE_API_KEY=sua-chave-aqui
USE_VERTEX_AI=false

GEMINI_THRESHOLD=0.5
```

> Para obter sua `GOOGLE_API_KEY`: acesse https://aistudio.google.com/app/apikey e clique em **"Create API Key"**.

### 6. Coloque os arquivos de credenciais

Copie os arquivos `key.json` e `firestore-key.json` para a raiz do projeto (mesma pasta do `main.py`).

### 7. Rode a API

```bash
python api.py
```

A API estará disponível em `http://localhost:8000`

Para testar, acesse `http://localhost:8000/docs` para ver a documentação interativa (Swagger UI).

Ou faça uma requisição:

```bash
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d '{"prompt": "liste propostas de veículos leves"}'
```

---

## Exemplos de perguntas

```
liste propostas de veículos leves
mostre propostas de motos para pessoa física
quero ver propostas com 5 parcelas
liste propostas aprovadas dos últimos 30 dias
mostre propostas de veículos leves com garantia
```

Para mais exemplos, veja `EXEMPLOS_QUERIES.md`.

---

## Problemas comuns

**Erro 429 - quota esgotada**
O modelo Gemini tem limite diário no plano gratuito. O agente tenta automaticamente outros modelos disponíveis. Se todos estiverem esgotados, aguarde até o dia seguinte ou acesse https://aistudio.google.com/app/apikey para verificar o uso.

**Erro de credenciais**
Verifique se os arquivos `key.json` e `firestore-key.json` estão na raiz do projeto e se os caminhos no `.env` estão corretos.

**Comando `python` não encontrado**
Tente usar `python3` no lugar de `python`.

---

## Estrutura do projeto

```
intent_agent/
├── api.py                   # Ponto de entrada — API REST
├── .env                     # Suas configurações (não compartilhe este arquivo)
├── key.json                 # Credenciais Vertex AI
├── firestore-key.json       # Credenciais Firestore
├── requirements.txt         # Dependências Python
├── 4_TABELAS_ESSENCIAIS.md  # Documentação das 4 tabelas base
├── API_README.md            # Documentação da API
├── EXEMPLOS_QUERIES.md      # Exemplos de queries
├── QUERY_PATTERNS.md        # Padrões de query SQL
└── src/
    ├── agent/               # Lógica principal do agente
    ├── dictionaries/        # Mapeamento de valores (códigos)
    ├── repositories/        # Acesso ao banco de dados
    ├── services/            # Comunicação com o Gemini e validações
    ├── strategies/          # Como os filtros são extraídos
    ├── models/              # Estrutura dos dados internos
    └── config/              # Configurações da aplicação
```

---

## Como funciona por dentro

1. Você faz uma requisição POST para `/query` com um prompt
2. O agente lê todas as tabelas disponíveis no repositório (Firestore ou JSON local)
3. Envia tudo para o Gemini em **uma única chamada**
4. O Gemini identifica qual tabela usar (sempre TbProduto) e quais filtros aplicar
5. O agente valida os filtros e monta o JSON final
6. O JSON é retornado na resposta da API

## Documentação Adicional

- `API_README.md` - Documentação completa da API REST
- `4_TABELAS_ESSENCIAIS.md` - As 4 tabelas que SEMPRE devem estar na query
- `QUERY_PATTERNS.md` - Padrões de query SQL e relacionamentos
- `EXEMPLOS_QUERIES.md` - 10 exemplos práticos de queries
