import os
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
from airflow.sensors.filesystem import FileSensor

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
ruta_archivo = AIRFLOW_HOME + '/dags/data/bquxjob_3b3102f3_18728d1eb06.csv'

def load_csv_to_mysql():
    ruta_archivo = AIRFLOW_HOME + '/dags/data/bquxjob_3b3102f3_18728d1eb06.csv'
    df = pd.read_csv(ruta_archivo)

    # Diccionario que mapea los nombres de columna de MySQL a los tipos de datos equivalentes de Python
    column_types = {
        'order_number': str,
        'order_status': str,
        'customer_email': str,
        'preferred_delivery_date': 'datetime64',
        'preferred_delivery_hours': str,
        'sales_person': str,
        'notes': str,
        'address': str,
        'neighbourhood': str,
        'city': str,
        'creation_date': 'datetime64',
        'source': str,
        'warehouse': str,
        'shopify_id': int,
        'sales_person_role': str,
        'order_type': str,
        'is_pitayas': str,
        'discount_applications': str,
        'payment_method': str
    }

    # Convertir las columnas al tipo de datos deseado
    df = df.astype(column_types)

    # Limpieza de faltantes
    df = df.applymap(lambda x: None if pd.isna(x) else x)
    df['preferred_delivery_date'] = df['preferred_delivery_date'].fillna(datetime(1900, 1, 1, 0, 0))

    # Conexion y descargue de tabla orders
    mysql_hook = MySqlHook(mysql_conn_id='azure_mysql_conn')
    engine = mysql_hook.get_sqlalchemy_engine()
    df_sql = pd.read_sql('SELECT * FROM orders', con=engine)
    df_sql = df_sql.add_suffix('_sql')
    df_sql = df_sql[['order_number_sql']]

    # Unir los DataFrames usando la columna 'order_number' y 'order_number_sql' como claves
    if not df_sql.empty:
        df_merge = pd.merge(df, df_sql, left_on='order_number', right_on='order_number_sql', how='left', indicator=True)
        df_new = df_merge[df_merge['_merge'] == 'left_only']
        df_new = df_new.applymap(lambda x: None if pd.isna(x) else x)
        df_new = df_new.drop('_merge', axis=1)
        df_new = df_new.drop('order_number_sql', axis=1)
        
        if not df_new.empty:
            print(df_new.values.tolist())
            mysql_hook.insert_rows(table='orders', rows=df_new.values.tolist(), replace=False)       
    else:
        mysql_hook.insert_rows(table='orders', rows=df.values.tolist(), replace=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}


with DAG(
    'load_csv_to_mysql_sensor',
    default_args=default_args,
    schedule_interval=None
) as dag:
    
    file_sensor = FileSensor(
        task_id='file_sensor_task',
        filepath= ruta_archivo,
        poke_interval=10
    )

    cargue_info = PythonOperator(
        task_id='load_csv_to_mysql',
        python_callable=load_csv_to_mysql)

    file_sensor >> cargue_info