import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 9, 1),
    'catchup': True,
}

# Define the DAG
dag = DAG(
    'forex_data_pipeline',
    default_args=default_args,
    schedule_interval='0 1 * * *',
    max_active_runs=1,
)


def get_api_key():
    return os.environ.get('FOREX_API_KEY')


def check_api_availability(date, **kwargs):
    base = os.environ.get("base")
    url = f"https://api.apilayer.com/exchangerates_data/{date}?{base}"
    headers = {"apikey": get_api_key()}
    response = requests.get(url, headers=headers)
    return response.status_code == 200


def create_forex_table():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cur:
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS forex_rates (
                    id SERIAL PRIMARY KEY,
                    currency VARCHAR(10),
                    rate NUMERIC,
                    date DATE,
                    UNIQUE (currency, date)
                );
            """
            cur.execute(create_table_sql)
            conn.commit()


def download_forex_rates(date, **kwargs):
    base = os.environ.get("base")
    url = f"https://api.apilayer.com/exchangerates_data/{date}?{base}"
    headers = {"apikey": get_api_key()}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data['rates'], index=[0]).T.reset_index()
        df.columns = ['currency', 'rate']
        df['date'] = date
        df.to_csv(f'forex_rates_{date}.csv', index=False)
        return df
    return None


def upsert_forex_data(**context):
    df = context['ti'].xcom_pull(task_ids='download_rates')
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO forex_rates (currency, rate, date)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (currency, date) DO UPDATE
                    SET rate = EXCLUDED.rate;
                """, (row['currency'], row['rate'], row['date']))
            conn.commit()


def analyze_data():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    df = postgres_hook.get_pandas_df("SELECT * FROM forex_rates")

    latest_date = df['date'].max()
    latest_rates = df[df['date'] == latest_date]

    results = []

    for currency in latest_rates['currency']:
        latest_rate = latest_rates.loc[
            latest_rates['currency'] == currency, 'rate'
        ].values[0]
        previous_rates = df[
            (df['currency'] == currency) & (df['date'] < latest_date)
        ]

        if not previous_rates.empty:
            previous_rates['difference'] = (previous_rates['rate'] - latest_rate).abs()
            max_diff_row = previous_rates.loc[previous_rates['difference'].idxmax()]
            results.append([
                latest_date,
                max_diff_row['date'],
                currency,
                max_diff_row['difference']
            ])

    results_df = pd.DataFrame(
        results,
        columns=['Scheduler_Run_Date', 'Max_Change_Date', 'Currency', 'Difference']
    )
    results_df.to_csv(f'{latest_date}_max_change.csv', index=False)


def send_csv_to_telegram():
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    with open(f'max_change.csv', 'rb') as file:
        requests.post(url, data={'chat_id': chat_id}, files={'document': file})


# Define the tasks
check_api = PythonOperator(
    task_id='check_api',
    python_callable=check_api_availability,
    op_kwargs={
        'date': '{{ (execution_date - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}'
    },
    provide_context=True,
    dag=dag
)

create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_forex_table,
    dag=dag
)

download_rates = PythonOperator(
    task_id='download_rates',
    python_callable=download_forex_rates,
    op_kwargs={
        'date': '{{ (execution_date - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}'
    },
    provide_context=True,
    dag=dag
)

upsert_rates = PythonOperator(
    task_id='upsert_rates',
    python_callable=upsert_forex_data,
    provide_context=True,
    dag=dag
)

analyze_data_task = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    dag=dag
)

send_csv_task = PythonOperator(
    task_id='send_csv',
    python_callable=send_csv_to_telegram,
    dag=dag
)

# Set task dependencies
(
    check_api
    >> create_table
    >> download_rates
    >> upsert_rates
    >> analyze_data_task
    >> send_csv_task
)
