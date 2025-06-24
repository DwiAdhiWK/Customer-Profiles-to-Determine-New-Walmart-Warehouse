from datetime import datetime
from sqlalchemy import create_engine
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import pandas as pd
from elasticsearch import Elasticsearch
# Tambahin data_raw.csv ke folder data 

default_args= {
    'owner': 'Dwi',
    'start_date': datetime(2024, 11, 1),
}

with DAG(
    'P2M3_dwi_adhi_DAG',
    description='from postgres to elasticsearch',
    schedule_interval='10-30/10 9 * * 6',
    default_args=default_args, 
    catchup=False) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task()
    def preprocess_data():
        # Method ini digunakan untuk cleaning data mentah yang diambil dari table_m3

        df = pd.read_csv('/opt/airflow/data/P2M3_dwi_adhi_data_raw.csv')
        
        # Handle missing values (example: fill missing numeric values with the mean)
        df.fillna(df.mean(numeric_only=True), inplace=True)
        
        # Remove duplicates if any
        df.drop_duplicates(inplace=True)

        # Convert all column to lower case
        df.columns = df.columns.str.lower()

        # Replace all white space into underscore        
        df.columns = df.columns.str.replace(' ', '_')

        # Remove all character before the first character and after last character
        df.columns = df.columns.str.strip()

        print("Preprocessed data is Success")
        print(df.head())
        df.to_csv('/opt/airflow/data/P2M3_dwi_adhi_data_clean.csv', index=False)

    @task()
    def fetch_from_postgre():
        # Method ini digunakan untuk mengambil data dari database dan membuat csv baru.
        database = "airflow"
        username = "airflow"
        password = "airflow"
        host = "postgres"

        postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

        engine = create_engine(postgres_url)
        conn = engine.connect()
        # ganti isi table_m3 dengan data dari kaggle 
        df = pd.read_sql('select * from table_m3',conn)
        df.to_csv('/opt/airflow/data/P2M3_dwi_adhi_data_raw.csv')
        #df.read_sql('data_customer', conn, index=False, if_exists='replace')
        print("Success FETCH")
        

    @task()
    def load_elasticsearch():
        # Method ini digunakan untuk load data yang sudah bersih ke elasticsearch untuk dibuat visualisasi.
        es = Elasticsearch("http://elasticsearch:9200") # localhost:9200
        df = pd.read_csv('/opt/airflow/data/P2M3_dwi_adhi_data_clean.csv')
        for i, r in df.iterrows():
            doc = r.to_json()
            res = es.index(index = "milestone3",doc_type = "doc", body=doc)
            print(res)

    start >> fetch_from_postgre() >> preprocess_data() >> load_elasticsearch() >> end