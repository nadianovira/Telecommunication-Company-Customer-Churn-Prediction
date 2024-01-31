    
'''
=================================================
Dataset : The dataset is represent the "Customer churn" on one of the telco company. 
"Customer Churn" is the percentage of customers that stopped using company's product 
or service during a certain time frame. It is more expensive to acquiring new customer 
than retain existing customer, which is make the most important metric to evaluate the growing business.
Source https://www.kaggle.com/code/krvivek03/telco-customer-churn-prediction-91-auc-score/input.

Objective : The telco customer can easily choose another telco company due to the similar services provided, 
in this highly competitive market. Thus the company would like to analyze the business using churn rate metrics, 
so they can maintain the existing customer and to address this the telecom companies should predict which 
customers are highly prone to churn. If the company could forecast which customers are likely to leave ahead of time, 
it could concentrate customer retention efforts only on these "high risk" clients.

This Program made for transform and load automation from PostgreSQL to ElasticSearch. 
=================================================
'''
# import libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.utils.task_group import TaskGroup
from datetime import datetime,timedelta
from sqlalchemy import create_engine #koneksi ke prostgres
import pandas as pd
import time
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import inflection
# pip install inflection

def load_csv_to_postgres():
    '''
    load file CSV ke tabel PostgreSQL.
    - Pengaturan Koneksi Database: database, username, password, dan host informasi koneksi ke database PostgreSQL. 
    - Membuat URL Koneksi PostgreSQL: URL ini akan digunakan untuk membuat objek koneksi SQLAlchemy.
    - Membuat Koneksi Engine SQLAlchemy: fungsi create_engine dari SQLAlchemy membuat objek engine yang digunakan untuk berkomunikasi dengan database PostgreSQL.
    - Membuka Koneksi ke Database: menggunakan engine.connect(), untuk membuka koneksi ke database PostgreSQL. Objek conn akan digunakan untuk mengeksekusi perintah SQL.
    - Membaca File CSV: file CSV yang dibaca berada di lokasi '/opt/airflow/dags/P2M3_nadia_data_raw.csv'.
    - Menyimpan Data ke Tabel PostgreSQL: data dari DataFrame Pandas (df) disimpan ke dalam tabel PostgreSQL dengan nama 'table_m'. 
    Fungsi ini kemudian diintegrasikan dalam alur kerja Apache Airflow untuk otomatisasi proses.
    '''
    database = "airflow"
    username = "airflow"
    password = "airflow"
    host = "postgres"
    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"
    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    #engine = create_engine("postgresql+psycopg2://{username}:{password}@{host}/{database}")
    conn = engine.connect()
    df = pd.read_csv('/opt/airflow/dags/P2M3_nadia_data_raw.csv') #sesuaikan dengan nama yg ada di airflow bagian volumes dags + nama filenya
    df.to_sql('table_m', conn, index=False, if_exists='replace')  # Menggunakan if_exists='replace' agar tabel digantikan jika sudah ada

def ambil_data():
    '''
    - Membuat URL Koneksi PostgreSQL: Sama seperti sebelumnya, URL koneksi PostgreSQL dibuat menggunakan informasi koneksi yang telah disediakan.
    - Membuat Koneksi Engine SQLAlchemy: Objek engine dibuat menggunakan URL koneksi PostgreSQL, dan kemudian engine.connect() digunakan untuk membuka koneksi ke database.
    - Mengambil Data dari Tabel PostgreSQL: fungsi read_sql_query digunakan untuk menjalankan perintah SQL yang mengambil semua data dari tabel dengan nama 'table_m'. 
        Hasil query disimpan dalam DataFrame Pandas (df).
    - Menyimpan Data ke File CSV: menggunakan df.to_csv, file CSV akan disimpan di lokasi '/opt/airflow/dags/P2M3_nadia_data_raw.csv', 
    Fungsi ini dapat digunakan untuk mengekstrak data dari tabel PostgreSQL dan menyimpannya dalam format file CSV. 
    Fungsi ini akan diintegrasikan dalam alur kerja Apache Airflow untuk otomatisasi proses pengambilan data dari database.
    '''
    # fetch data
    database = "airflow"
    username = "airflow"
    password = "airflow"
    host = "postgres"
    # Membuat URL koneksi PostgreSQL
    postgres_url1 = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"
    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url1)
    conn = engine.connect()
    df = pd.read_sql_query("select * from table_m", conn) #nama table sesuaikan dengan nama table di postgres
    df.to_csv('/opt/airflow/dags/P2M3_nadia_data_raw.csv', sep=',', index=False)

def preprocessing(): 
    '''
    Fungsi preprocessing ini dirancang untuk melakukan beberapa tahapan pembersihan (preprocessing) pada data yang dibaca dari file CSV.
    - Membaca Data dari File CSV: membaca data dari file CSV yang berada di lokasi '/opt/airflow/dags/P2M3_nadia_data_raw.csv'.
    - Mengganti Tipe Data Kolom 'TotalCharges' ke Float: spasi diubah menjadi '0'. Setelah itu, menggunakan astype(float), tipe data kolom 'TotalCharges' diubah menjadi float.
    - Merubah Nama Kolom: Menggunakan inflection, nama kolom diubah menjadi format snake_case (underscore). Misalnya, jika nama kolom awalnya adalah 'TotalCharges', setelah proses ini akan menjadi 'total_charges'.
    - Cleaning Data: Menggunakan dropna, data yang mengandung nilai-nilai null dihapus. Selanjutnya, menggunakan drop_duplicates, baris duplikat dihapus dari data.
    - Menyimpan Data new ke File CSV:
    - Menggunakan to_csv, data yang telah melalui proses preprocessing disimpan ke dalam file CSV baru. File CSV ini akan berada di lokasi '/opt/airflow/dags/P2M3_nadia_data_clean.csv', dan indeks tidak disertakan.
    Fungsi ini dapat digunakan untuk membersihkan dan mempersiapkan data untuk analisis lebih lanjut setelah proses preprocessing selesai. 
    Fungsi ini akan diintegrasikan ke dalam alur kerja Apache Airflow untuk otomatisasi.
    '''
    # Read data
    data = pd.read_csv("/opt/airflow/dags/P2M3_nadia_data_raw.csv")
    # Change type data to float
    data['TotalCharges'] =data['TotalCharges'].str.replace(' ','0')
    data['TotalCharges'] = data['TotalCharges'].astype(float)
    # Underscore and lower
    data.columns = [inflection.underscore(col) for col in data.columns]
    # data.columns.str.lower()
    # bersihkan data 
    data.dropna(inplace=True)
    data.drop_duplicates(inplace=True)
    data.to_csv('/opt/airflow/dags/P2M3_nadia_data_clean.csv', index=False)


def upload_to_elasticsearch():

    '''
    Fungsi upload_to_elasticsearch ini bertujuan untuk mengunggah data yang telah diproses ke Elasticsearch. 
    - Membuat Koneksi ke Elasticsearch:
    - Menggunakan pustaka elasticsearch, fungsi Elasticsearch("http://elasticsearch:9200") digunakan untuk membuat koneksi ke Elasticsearch.
    - Membaca Data dari File CSV: membaca data dari file CSV yang berada di lokasi '/opt/airflow/dags/P2M3_nadia_data_clean.csv'.
    - Persiapan Data untuk Pengindeksan Paket Besar (Bulk Indexing): Membuat list actions yang berisi data yang siap untuk diindeks ke Elasticsearch. 
    Setiap item dalam list actions merupakan sebuah dictionary yang berisi informasi-informasi untuk proses indexing, seperti index, id, dan sumber data.
    - Bulk Indexing ke Elasticsearch: Menggunakan fungsi bulk dari pustaka elasticsearch.helpers, melakukan pengindeksan sekaligus (bulk indexing) ke Elasticsearch. 
    Data diindeks ke index dengan nama "table_update".
    - Prin jumlah dokumen yang berhasil diindeks dan jumlah dokumen yang gagal diindeks ke dalam Elasticsearch.
    Jadi, fungsi ini dapat digunakan untuk mentransfer data dari file CSV yang telah di-preprocess ke dalam Elasticsearch, 
    memungkinkan data tersebut untuk dicari dan dianalisis menggunakan fitur-fitur Elasticsearch. 
    Fungsi ini kemudian diintegrasikan dalam alur kerja Apache Airflow untuk otomatisasi proses pengunggahan data ke Elasticsearch.
    
    '''
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_nadia_data_clean.csv')
    # Persiapkan data untuk pengindeksan paket besar
    actions = [
        {"_op_type": "index", "_index": "table_update", "_id": i+1, "_source": doc.to_dict()}
        for i, doc in df.iterrows()
    ]
    # Gunakan bulk indexing untuk mengirimkan data sekaligus
    success, failed = bulk(es, actions=actions, index="table_update")
    print(f"Successfully indexed: {success}")




default_args = {
    'owner': 'nadia',
    'start_date': datetime(2024, 1, 24, 14, 4) - timedelta (hours = 7)
}
# mendefine dag agar bisa dibaca di ariflow dengan penjadwalan setiap jam 6.30
with DAG(
    "P2M3_nadia_DAG", #sesuaikan nama project
    description='Milestone_3',
    schedule_interval='30 6 * * *', # dimulai setiap jam 06.30
    default_args=default_args, 
    catchup=False
) as dag:
    
    # Task 1
    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres) #sesuaikan dengan nama fungsi
    # Task 2
    ambil_data_pg = PythonOperator(
        task_id='ambil_data_to_postgres',
        python_callable=ambil_data) #sesuaikan dengan nama fungsi
    # Task 3
    pre_processing = PythonOperator(
        task_id='preprocess_to_postgres',
        python_callable=preprocessing) #sesuaikan dengan nama fungsi
    # Task 4
    Upload_elastic = PythonOperator(
        task_id='elasticsearch_to_postgres',
        python_callable=upload_to_elasticsearch) #sesuaikan dengan nama fungsi
    
    # untuk menjalankan airflow
    load_csv_task >> ambil_data_pg >> pre_processing >> Upload_elastic

