from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'XYZ Telecoms',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define Postgres connection details
pg_host = '104.131.120.201'
pg_database = 'call_log_db'
pg_user = 'postgres'
pg_password = 'pg_W33k8'

dag = DAG('data_pipeline', default_args=default_args, schedule_interval='@daily')

def extract_data():
    # extract data from CSV files
    # load the CSV data into Pandas dataframes for later transformation
    customer_data_df = pd.read_csv('https://raw.githubusercontent.com/wambasisamuel/DE_Week08_Monday/main/customer_data.csv')
    order_data_df = pd.read_csv('https://raw.githubusercontent.com/wambasisamuel/DE_Week08_Monday/main/order_data.csv')
    payment_data_df = pd.read_csv('https://raw.githubusercontent.com/wambasisamuel/DE_Week08_Monday/main/payment_data.csv')
    # return dataframes as a dictionary
    return {'customer_data': customer_data_df, 'order_data': order_data_df, 'payment_data': payment_data_df}

def transform_data(**context): 
    # retrieve dataframes from context
    customer_df = context['task_instance'].xcom_pull(task_ids='extract_data')['customer_data']
    order_df = context['task_instance'].xcom_pull(task_ids='extract_data')['order_data']
    payment_df = context['task_instance'].xcom_pull(task_ids='extract_data')['payment_data']

    # convert date fields to the correct format using pd.to_datetime
    customer_df['date_of_birth'] = pd.to_datetime(customer_df['date_of_birth'])
    order_df['order_date'] = pd.to_datetime(order_df['order_date'])
    payment_df['payment_date'] = pd.to_datetime(payment_df['payment_date'])

    # merge customer and order dataframes on the customer_id column
    merged_df = pd.merge(customer_df, order_df, on='customer_id')

    # merge payment dataframe with the merged dataframe on the order_id and customer_id columns
    final_df = pd.merge(merged_df, payment_df, on=['order_id', 'customer_id'])

    # drop unnecessary columns like customer_id and order_id
    final_df.drop(['customer_id', 'order_id'], axis=1, inplace=True)

    # group the data by customer and aggregate the amount paid using sum
    agg_df = final_df.groupby('customer_name').agg({'amount_paid': 'sum'})

    # create a new column to calculate the total value of orders made by each customer
    agg_df['total_order_value'] = final_df.groupby('customer_name').agg({'order_value': 'sum'})

    # calculate the customer lifetime value using the formula CLV = (average order value) x (number of orders made per year) x (average customer lifespan) 
    agg_df['lifespan'] = (max(final_df['payment_date']) - min(final_df['payment_date'])).days / 365
    agg_df['purchase_frequency'] = final_df.groupby('customer_name').size() / len(final_df['payment_date'].unique())
    agg_df['avg_order_value'] = agg_df['total_order_value'] / agg_df['purchase_frequency']
    agg_df['clv'] = (agg_df['avg_order_value'] * agg_df['purchase_frequency']) * agg_df['lifespan']
                    
    return agg_df

def load_data(df):
    # load the transformed data into Postgres database
    # Connect to Postgres
    conn = psycopg2.connect(host=pg_host, database=pg_database, user=pg_user, password=pg_password)
    
    df.to_sql('airflow', con=conn, if_exists='replace',index=False)
    conn.autocommit = True


# define DAG parameters
default_args = {
    'owner': 'MTN Rwanda',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define DAG
dag = DAG(
    'mtn_data_pipeline',
    default_args=default_args,
    description='Data pipeline to extract, transform, and load data from three CSV files and store it in a Postgres database'
    schedule_interval=timedelta(days=1),
)


# Task definition
# define tasks
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Task dependencies
transform_data_task.set_upstream(extract_data_task)
load_data_task.set_upstream(transform_data_task)

with dag:
    
    # extract data 
    extract_data()

    # transform data 
    transform_data()

    # load data 
    load_data(transform_data)

    # define dependencies extract_data >> transform_data >> load_data
    transform_data_task.set_upstream(extract_data_task)
    load_data_task.set_upstream(transform_data_task)
