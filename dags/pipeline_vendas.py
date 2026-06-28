import logging
import requests
import datetime
from airflow.decorators import dag, task_group, task
from airflow.exceptions import AirflowException
import pendulum
import pandas as pd
from validadores import ValidarCategoriasOperator, ValidarMetricasProdutosOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def alert_failure(context):
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    logging.error(f"🚨 ALERTA DE PRODUÇÃO: A task {task_id} FALHOU CRITICAMENTE em {execution_date}!")

def alert_retry(context):
    task_id = context.get('task_instance').task_id
    try_number = context.get('task_instance').try_number
    logging.warning(f"⚠️ Instabilidade detectada na task {task_id}. Tentativa número {try_number}. Aplicando backoff...")

def alertar_sucesso(context):
    task_id = context.get('task_instance').task_id
    logging.info(f"✅ Sucesso absoluto na execução da task {task_id}.")


# --- DEFINIÇÃO DA DAG OBRIGATÓRIA ---
@dag(
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"), # Timezone obrigatório do projeto
    schedule_interval="0 6 * * *", # Rodar todo dia às 06:00
    catchup=False,
    tags=['shopbrasil']
)
def pipeline_shopbrasil():

    @task_group(group_id="ingest")
    def tg_ingest():
        
        @task(
            retries=3,
            retry_delay=datetime.timedelta(seconds=5),
            retry_exponential_backoff=True,
            on_failure_callback=alert_failure,
            on_retry_callback=alert_retry,
            on_success_callback=alertar_sucesso
        )
        def get_categories_api():
            # Recomendação: Voltar para https://fakestoreapi.com/products/categories
            url = "https://api.escuelajs.co/api/v1/categories" 
            
            try:
                logging.info("Iniciando requisição na API...")
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                categories = response.json()
                
                logging.info(f"Carga de {len(categories)} categorias capturada com sucesso.")
                slugs = [category['id'] for category in categories]
                return slugs
                
            except requests.RequestException as err:
                logging.error(f"Erro na comunicação com a API: {str(err)}")
                raise AirflowException(f"API indisponível. Forçando retentativa. Erro original: {err}")

        categories = get_categories_api()
        
        # Validar categorias capturadas
        validar_categorias = ValidarCategoriasOperator(
            task_id='validar_categorias',
            categorias=categories
        )
        
        categories >> validar_categorias
        return categories
    
    @task_group(group_id="analisis")
    def tg_analisis(categories):
        
        @task(
            pool="ecommerce_pool",
            retries=2,
            retry_delay=datetime.timedelta(seconds=10),
            retry_exponential_backoff=True,
            on_failure_callback=alert_failure,
            on_retry_callback=alert_retry,
            on_success_callback=alertar_sucesso
        )
        def calculate_category_metrics(category_id):
            logging.info(f"Chamando API para produtos de categoria {category_id}")
            url = f"https://api.escuelajs.co/api/v1/categories/{category_id}/products"

            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                products = response.json()
            except requests.RequestException as err:
                logging.error(f"Erro ao buscar produtos da categoria {category_id}: {str(err)}")
                raise AirflowException(f"Falha ao buscar categoria {category_id}. Erro: {err}")

            logging.info("Iniciando análise das categorias...")    
            if not products:
                logging.warning(f"Categoria '{category_id}' não possui produtos.")
                return {
                    "categoria": category_id,
                    "preco_medio": None,
                    "preco_minimo": None,
                    "preco_maximo": None,
                    "quantidade_produtos": 0
                }
            
            df = pd.DataFrame(products)
            
            metricas = {
                "categoria": category_id,
                "preco_medio": float(df['price'].mean()),
                "preco_minimo": float(df['price'].min()),
                "preco_maximo": float(df['price'].max()),
                "quantidade_produtos": int(df['id'].count())
            }
            
            logging.info(f"Métricas calculadas para {category_id}: {metricas}")
            return metricas

        metrics_results = calculate_category_metrics.expand(category_id=categories)
        return metrics_results

    @task_group(group_id="load_metrics")
    def tg_load_metrics(metrics):

        @task(
            retries=2,
            retry_delay=datetime.timedelta(seconds=10),
            retry_exponential_backoff=True,
            on_failure_callback=alert_failure,
            on_retry_callback=alert_retry,
            on_success_callback=alertar_sucesso
        )
        def salvar_no_banco(metricas_list):
            hook = PostgresHook(postgres_conn_id='postgres_default')
    
            for metrica in metricas_list:
                sql = """
                    INSERT INTO analise_produtos (categoria, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (categoria) 
                    DO UPDATE SET 
                        preco_medio = EXCLUDED.preco_medio,
                        preco_minimo = EXCLUDED.preco_minimo,
                        preco_maximo = EXCLUDED.preco_maximo,
                        quantidade_produtos = EXCLUDED.quantidade_produtos;
                """
                hook.run(sql, parameters=(
                    metrica['categoria'], 
                    metrica['preco_medio'], 
                    metrica['preco_minimo'], 
                    metrica['preco_maximo'], 
                    metrica['quantidade_produtos']
                ))

        return salvar_no_banco(metricas_list=metrics)
    # Fluxo principal da DAG
    categories = tg_ingest()
    metrics = tg_analisis(categories)
    tg_load_metrics(metrics)

# --- INSTANCIAÇÃO DA DAG (MUITO IMPORTANTE) ---
# Sem essa linha final, o Airflow não enxerga a DAG
pipeline_shopbrasil()