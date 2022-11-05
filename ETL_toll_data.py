#imports
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#DAG args
default_args = {
    'owner' : 'Rishabh Singh',
    'start_date' : days_ago(0),
    'email' : 'abc@xyz.com',
    'email_on_failure': True,
    'email_on_retry' : True,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}

#DAG Definition
dag = DAG(
    'ETL_toll_data',
    default_args = default_args,
    description = 'Apache Airflow Final Assignment', 
    schedule_interval = "@daily",
)

#Create unzip task
unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -xvf tolldata.tgz -C /home/project/airflow/dags/finalassignment/',
    dag = dag,
)

#Extract from CSV file
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d"," -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag = dag,
)

#Extract from TSV file
extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -f5,6,7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv | tr -d "\r" > /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag = dag,
)

#Extract from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'cut -c 59-67 /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag = dag,
)

#Consolidate data
consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste csv_data.csv tsv_data.csv fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag = dag,
)

#Transform data
transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = 'cut -d"," -f4 extracted_data.csv --complement > tmp1.csv | cut -d"," -f4 extracted_data.csv | tr "[a-z]" "[A-Z]" > tmp2.csv| paste tmp1.csv tmp2.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv',
    dag = dag,
)

#Task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data