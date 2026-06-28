# Pipeline de Orquestração de Workflows - ShopBrasil

Pipeline de ETL para coleta, análise e armazenamento de métricas de produtos por categoria de um e-commerce.

---

## 📋 Pré-requisitos

- **Docker** >= 20.10
- **Docker Compose** >= 1.29
- **Python** >= 3.9
- **PostgreSQL** (via Docker Compose)
- **Apache Airflow** >= 2.6 (via Docker)

---

## 🚀 Instalação e Setup

### 1. Clonar o repositório

```bash
git clone <repo_url>
cd orquestracao_workflows
```

### 2. Inicializar o ambiente Docker

```bash
docker-compose up -d
```

Este comando irá:
- Inicializar o PostgreSQL
- Executar scripts de inicialização do banco de dados
- Inicializar o Apache Airflow com todos os componentes necessários

### 3. Acessar a UI do Airflow

```
http://localhost:8080
```

**Credenciais padrão:**
- Usuário: `airflow`
- Senha: `airflow`

---

## ⚙️ Configurações Obrigatórias no Airflow

### 1. Criar Pool `ecommerce_pool`

A task `calculate_all_metrics` requer um pool para controlar paralelismo.

**Passos na UI do Airflow:**

1. Acesse: **Admin** → **Pools**
2. Clique em **+ Create**
3. Preencha os campos:
   - **Pool**: `ecommerce_pool`
   - **Slots**: `5` (ajuste conforme sua capacidade)
   - **Description**: `Pool para limitar paralelismo de requisições à API de categorias`
4. Clique em **Save**

### 2. Configurar Conexão PostgreSQL (postgres_default)

A conexão com o banco de dados já vem pré-configurada, mas você pode validar:

1. Acesse: **Admin** → **Connections**
2. Procure por `postgres_default`
3. Verifique os dados:
   - **Connection Type**: `Postgres`
   - **Host**: `postgres`
   - **Port**: `5432`
   - **Database**: `airflow`
   - **Login**: `airflow`
   - **Password**: `airflow`

> **Nota**: Se estiver usando um PostgreSQL externo, atualize a conexão com os dados correspondentes.

---

## 📦 Estrutura do Projeto

```
orquestracao_workflows/
├── dags/
│   └── pipeline_vendas.py          # DAG principal
├── plugins/
│   ├── __init__.py
│   └── validadores.py              # Operadores customizados
├── docker-entrypoint-initdb.d/
│   └── init.sql                    # Script de inicialização do banco
├── docker-compose.yaml             # Configuração dos containers
├── Dockerfile                      # Imagem do Airflow
├── requirements.txt                # Dependências Python
└── README.md                       # Este arquivo
```

---

## 🔄 Topologia da DAG

```
ingest
  ├─ get_categories_api (Task)
  └─ validar_categorias (ValidarCategoriasOperator)
       │
analisis
  └─ calculate_all_metrics (Task)
       │
load_metrics
  └─ salvar_no_banco (Task)
```

**Fluxo de dados:**

1. `get_categories_api` → extrai categorias da API
2. `validar_categorias` → valida integridade
3. `calculate_all_metrics` → calcula métricas por categoria
4. `salvar_no_banco` → insere dados no PostgreSQL

---

## 📊 Detalhes das Tasks

| Task | Objetivo | Retries | Pool | Timeout |
|------|----------|---------|------|---------|
| `get_categories_api` | Buscar categorias da API | 3 | - | 10s |
| `validar_categorias` | Validar integridade | 0 | - | - |
| `calculate_all_metrics` | Calcular métricas | 2 | `ecommerce_pool` | 10s |
| `salvar_no_banco` | Upsert no PostgreSQL | 2 | - | 10s |

**Métricas calculadas por categoria:**
- Preço médio
- Preço mínimo
- Preço máximo
- Quantidade de produtos

---

## 📈 Agendamento

- **Frequência**: Diariamente
- **Horário**: 06:00 (Timezone: America/Sao_Paulo)
- **Data de início**: 2026-01-01
- **Catchup**: Desativado

Para alterar, edite a linha `schedule_interval` em `dags/pipeline_vendas.py`.

---

## 🔍 Monitoramento

### Acessar logs

1. Na UI do Airflow, clique em uma execução da DAG
2. Selecione a task desejada
3. Navegue até **Logs**

### Identificadores de status

- `🚨 ALERTA DE PRODUÇÃO` → Falha crítica
- `⚠️ Instabilidade detectada` → Retry aplicado
- `✅ Sucesso absoluto` → Task concluída

### Consultar dados inseridos

```sql
SELECT * FROM analise_produtos ORDER BY categoria;
```

---

## 🛠️ Troubleshooting

### Pool não existe

```
Error: Pool 'ecommerce_pool' does not exist
```

**Solução**: Crie o pool em **Admin** → **Pools** conforme instruído acima.

### Conexão PostgreSQL recusada

```
Error: could not connect to server
```

**Solução**: Verifique o status dos containers:

```bash
docker-compose ps
docker-compose restart postgres
```

### DAG não aparece

**Solução**: Aguarde 5 minutos ou reinicie a UI:

```bash
docker-compose restart airflow-webserver
```

---

## 🔐 Segurança

- Credenciais padrão são **apenas para desenvolvimento**
- Para produção, use variáveis de ambiente seguras
- Não commite senhas no repositório

---

**Última atualização**: 26 de junho de 2026
