# plugins/validadores.py
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
import logging

class ValidarProdutosOperator(BaseOperator):
    # O template_fields permite que o Airflow substitua dinamicamente 
    # valores antes da execução da task.
    template_fields = ('produto',)

    def __init__(self, produto, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.produto = produto

    def execute(self, context):
        self.log.info(f"Validando contrato do produto: {self.produto.get('id', 'ID desconhecido')}")
        
        # Campos obrigatórios definidos pela regra de negócio
        campos_obrigatorios = ['id']
        
        for campo in campos_obrigatorios:
            if campo not in self.produto or self.produto[campo] is None:
                raise AirflowException(f"Falha na validação: Campo '{campo}' faltando ou nulo.")
        
        self.log.info("Validação concluída com sucesso.")
        return True