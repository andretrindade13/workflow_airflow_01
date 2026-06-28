# plugins/validadores.py
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

class ValidarCategoriasOperator(BaseOperator):
    """
    Valida se a lista de categorias (ids) capturadas da API é válida.
    Campos obrigatórios: categoria não pode ser nula.
    """
    template_fields = ('categorias',)

    def __init__(self, categorias, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.categorias = categorias

    def execute(self, context):
        categorias_list = self.categorias if isinstance(self.categorias, list) else [self.categorias]
        
        self.log.info(f"Validando {len(categorias_list)} categorias...")
        
        if not categorias_list:
            raise AirflowException("Lista de categorias está vazia. Abortando.")
        
        # Validação: categoria não pode ser nula
        for idx, cat_id in enumerate(categorias_list):
            if cat_id is None or (isinstance(cat_id, str) and cat_id.strip() == ''):
                raise AirflowException(f"Falha na validação: Categoria no índice {idx} tem id nulo ou vazio.")
        
        self.log.info(f"✅ Validação de categorias OK. {len(categorias_list)} categorias aprovadas.")
        return True


class ValidarMetricasProdutosOperator(BaseOperator):
    """
    Valida se a lista de métricas de produtos possui todos os campos obrigatórios.
    Campos obrigatórios: 'categoria', 'preco_medio', 'preco_minimo', 'preco_maximo', 'quantidade_produtos'
    """
    template_fields = ('metricas',)

    def __init__(self, metricas, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metricas = metricas

    def execute(self, context):
        metricas_list = self.metricas if isinstance(self.metricas, list) else [self.metricas]
        
        self.log.info(f"Validando {len(metricas_list)} métricas de produtos...")
        
        if not metricas_list:
            raise AirflowException("Lista de métricas está vazia. Abortando.")
        
        campos_obrigatorios = ['categoria', 'preco_medio', 'preco_minimo', 'preco_maximo', 'quantidade_produtos']
        
        for idx, metrica in enumerate(metricas_list):
            if not isinstance(metrica, dict):
                raise AirflowException(f"Falha na validação: Métrica no índice {idx} não é um dicionário.")
            
            for campo in campos_obrigatorios:
                if campo not in metrica:
                    raise AirflowException(f"Falha na validação: Campo '{campo}' faltando na métrica do índice {idx}.")
        
        self.log.info(f"✅ Validação de métricas OK. {len(metricas_list)} métricas aprovadas.")
        return True