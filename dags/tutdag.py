from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import json

def captura_conta_dados():
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len(df.index)
    return qtd

def e_valido(ti):
    qtd = ti.xcom_pull(task_ids = 'captura_conta_dados')
    if (qtd > 1000):
        return 'valido'
    return 'nvalido'

with DAG(dag_id='tutdag',
        default_args=default_args,
        description='',
        start_date=datetime(2023,7,1),
        schedule_interval='30****',
        catchup=False),
        as dag:
        captura_conta_dados = PythonOperator(
            task_id = 'captura_conta_dados',
            python_callable = captura_conta_dados
        )
        
        e_valido = BranchPythonOperator(
            task_id="e_valido",
            python_callable = e_valido,
            # op_kwargs: Optional[Dict] = None,
            # op_args: Optional[List] = None,
            # templates_dict: Optional[Dict] = None
            # templates_exts: Optional[List] = None
        )
        
        first_branch = DummyOperator(
            task_id='first_branch'
        )
        second_branch = DummyOperator(
            task_id='second_branch'
        )
        
        python_task >> [first_branch, second_branch]
        
        def e_valido(ti):
            qtd = ti.xcom_pull(task_ids = 'captura_conta_dados')
            if (qtd > 1000):
                return 'valida'
            return 'nvalida'
                    
        valido = BashOperator(
            task_id="(valido)",
            bash_command='echo "Quantidade OK"',
            # env: Optional[Dict[str, str]] = None,
            # output_encoding: str = 'utf-8',
            # skip_exit_code: int = 99,
        )
        
        nvalido = BashOperator(
            task_id="nvalido",
            bash_command='echo "Quantidade não OK"',
            # env: Optional[Dict[str, str]] = None,
            # output_encoding: str = 'utf-8',
            # skip_exit_code: int = 99,
        )


        captura_conta_dados >> e_valida >> [valido, nvalido]