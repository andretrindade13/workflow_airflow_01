import logging
import requests
import datetime
from airflow.decorators import dag, task_group, task
from airflow.exceptions import AirflowException
import pendulum
import pandas as pd

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
                response = requests.get(url, timeout=10) # Corrigido: timeout minúsculo

                response.raise_for_status() # Corrigido: raise_for_status
                categories = response.json()
                
                logging.info(f"Carga de {len(categories)} categorias capturada com sucesso.")
                slugs = [category['id'] for category in categories]
                return slugs
                
            except requests.RequestException as err:
                logging.error(f"Erro na comunicação com a API: {str(err)}")
                raise AirflowException(f"API indisponível. Forçando retentativa. Erro original: {err}")

        # Chamada da task dentro do grupo
        return get_categories_api()
    
    @task_group(group_id="analisis")
    def tg_analisis(categories):
        
        @task(pool="ecommerce_pool")
        def calculate_category_metrics(category_id):
           
            logging.info(f"Chamando APi para produtos de categoria {category_id}")
            url = f"https://api.escuelajs.co/api/v1/categories/{category_id}/products"

            response = requests.get(url, timeout=10)
            response.raise_for_status()
            products = response.json()

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

    # Fluxo principal da DAG
    categories = tg_ingest()
    metrics = tg_analisis(categories)

# --- INSTANCIAÇÃO DA DAG (MUITO IMPORTANTE) ---
# Sem essa linha final, o Airflow não enxerga a DAG
pipeline_shopbrasil()