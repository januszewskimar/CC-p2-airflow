from datetime import timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import pandas
from pymongo import MongoClient

import json




def procesaTemperatureHumidity():
    df_humidity = pandas.read_csv('/tmp/p2/humidity.csv')
    df_humidity = df_humidity[['datetime', 'San Francisco']]
    df_humidity = df_humidity.rename(columns={'datetime': 'DATE', 'San Francisco': 'HUM'})
    
    df_temperature = pandas.read_csv('/tmp/p2/temperature.csv')
    df_temperature = df_temperature[['datetime', 'San Francisco']]
    df_temperature = df_temperature.rename(columns={'datetime': 'DATE', 'San Francisco': 'TEMP'})
    
    df_resultado = pandas.merge(df_humidity, df_temperature, on="DATE");
    df_resultado.to_csv('/tmp/p2/resultado.csv')


def insertarEnBD():
    df = pandas.read_csv('/tmp/p2/resultado.csv')
    client = MongoClient('mongodb://root:pass@localhost:27017/')
    db = client.p2
    collection = db.sanfrancisco
    collection.insert_many(json.loads(df.to_json(orient='records')))
    



default_args = {
    'owner': 'Marcin Januszewski',
    'depends_on_past': False,
    'email': ['januszewskimar@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
with DAG(
    'practica2',
    default_args=default_args,
    description='Flujo de la práctica 2',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['practica2'],

) as dag:

    PrepararEntorno = BashOperator(
        task_id='preparar_entorno',
        bash_command='rm -rf /tmp/p2',
        dag=dag,
        depends_on_past=False,
    )

    DescargarHumidity = BashOperator(
        task_id='descargar_humidity',
        bash_command='curl -o /tmp/p2/humidity.csv.zip --create-dirs https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/humidity.csv.zip',
        dag=dag,
    )
    
    DescargarTemperature = BashOperator(
        task_id='descargar_temperature',
        bash_command='curl -o /tmp/p2/temperature.csv.zip --create-dirs https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/temperature.csv.zip',
        dag=dag,
    )
    
    DescomprimirHumidity = BashOperator(
        task_id='descomprimir_humidity',
        bash_command='unzip -o /tmp/p2/humidity.csv.zip -d /tmp/p2/',
        dag=dag,
    )
    
    DescomprimirTemperature = BashOperator(
        task_id='descomprimir_temperature',
        bash_command='unzip -o /tmp/p2/temperature.csv.zip -d /tmp/p2/',
        dag=dag,
    )
    
    ProcesarTemperatureHumidity = PythonOperator(
        task_id='procesar_temperature_humidity',
        python_callable=procesaTemperatureHumidity,
        dag=dag,
    )
    
    LanzarContenedorBD = BashOperator(
        task_id='lanzar_contenedor_bd',
        bash_command='~/airflow/dags/scripts/lanzar_contenedor_bd.sh ',
        dag=dag,
        depends_on_past=False,
    )
    
    EsperarBD = BashOperator(
        task_id='esperar_bd',
        bash_command='until nc -z 0.0.0.0 27017 2> /dev/null; do sleep 1; done',
        dag=dag,
    )
    
    InsertarEnBD = PythonOperator(
        task_id='insertar_en_bd',
        python_callable=insertarEnBD,
        dag=dag,
    )
    
    DescargarServicioV1 = BashOperator(
        task_id='descargar_servicio_v1',
        bash_command='git clone https://github.com/januszewskimar/CC-p2-v1 /tmp/p2/v1',
        dag=dag,
    )
    
    DescargarServicioV2 = BashOperator(
        task_id='descargar_servicio_v2',
        bash_command='git clone https://github.com/januszewskimar/CC-p2-v2 /tmp/p2/v2',
        dag=dag,
    )
    
    CopiarDatosServicioV1 = BashOperator(
        task_id='copiar_datos_servicio_v1',
        bash_command='cp /tmp/p2/{humidity.csv,temperature.csv} /tmp/p2/v1',
        dag=dag,
    )
    
    CopiarDatosServicioV2 = BashOperator(
        task_id='copiar_datos_servicio_v2',
        bash_command='cp /tmp/p2/{humidity.csv,temperature.csv} /tmp/p2/v2',
        dag=dag,
    )
    
    InstalarDependenciasServicioV1 = BashOperator(
        task_id='instalar_dependencias_servicio_v1',
        bash_command='pip install -r /tmp/p2/v1/requirements.txt',
        dag=dag,
    )
    
    InstalarDependenciasServicioV2 = BashOperator(
        task_id='instalar_dependencias_servicio_v2',
        bash_command='pip install -r /tmp/p2/v2/requirements.txt',
        dag=dag,
    )
    
    TestarServicioV1 = BashOperator(
        task_id='testar_servicio_v1',
        bash_command='cd /tmp/p2/v1; python test_api.py',
        dag=dag,
    )

    TestarServicioV2 = BashOperator(
        task_id='testar_servicio_v2',
        bash_command='cd /tmp/p2/v2; python test_api.py',
        dag=dag,
    )
    
    LimpiarContenedorServicioV1 = BashOperator(
        task_id='limpiar_contenedor_servicio_v1',
        bash_command='docker stop p2_1 2> /dev/null; docker rm p2_1 2> /dev/null',
        dag=dag,
    )

    LimpiarContenedorServicioV2 = BashOperator(
        task_id='limpiar_contenedor_servicio_v2',
        bash_command='docker stop p2_2 2> /dev/null; docker rm p2_2 2> /dev/null',
        dag=dag,
    )
    
    ConstruirImagenServicioV1 = BashOperator(
        task_id='construir_imagen_servicio_v1',
        bash_command='cd /tmp/p2/v1; docker build -t p2_v1 .',
        dag=dag,
    )
    
    ConstruirImagenServicioV2 = BashOperator(
        task_id='construir_imagen_servicio_v2',
        bash_command='cd /tmp/p2/v2; docker build -t p2_v2 .',
        dag=dag,
    )
    
    DesplegarContenedorServicioV1 = BashOperator(
        task_id='desplegar_contenedor_servicio_v1',
        bash_command='docker run -d -p 5000:5000 --name p2_v1 p2_v1',
        dag=dag,
    )
    
    DesplegarContenedorServicioV2 = BashOperator(
        task_id='desplegar_contenedor_servicio_v2',
        bash_command='docker run -d -p 5001:5000 --name p2_v2 p2_v2',
        dag=dag,
    )



PrepararEntorno >> [DescargarHumidity, DescargarTemperature]    
DescargarHumidity >> DescomprimirHumidity
DescargarTemperature >> DescomprimirTemperature
[DescomprimirHumidity, DescomprimirTemperature] >> ProcesarTemperatureHumidity
LanzarContenedorBD >> EsperarBD
[EsperarBD, ProcesarTemperatureHumidity] >> InsertarEnBD
InsertarEnBD >> [DescargarServicioV1, DescargarServicioV2]
DescargarServicioV1 >> [CopiarDatosServicioV1, InstalarDependenciasServicioV1]
DescargarServicioV2 >> [CopiarDatosServicioV2, InstalarDependenciasServicioV2]
[CopiarDatosServicioV1, InstalarDependenciasServicioV1] >> TestarServicioV1 >> LimpiarContenedorServicioV1 >> ConstruirImagenServicioV1 >> DesplegarContenedorServicioV1
[CopiarDatosServicioV2, InstalarDependenciasServicioV2] >> TestarServicioV2 >> LimpiarContenedorServicioV2 >> ConstruirImagenServicioV2 >> DesplegarContenedorServicioV2
