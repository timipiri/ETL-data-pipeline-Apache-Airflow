# import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# defining DAG arguments
default_args = {
    'owner': 'Timipiri Godgift',
    'start_date': days_ago(0),
    'email': ['dummy@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks

# define the first task
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command="tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz",
    dag=dag,
)

# define the second task
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command=(
        "awk -F',' '{print $1, $2, $3, $4}' "
        "/home/project/airflow/dags/finalassignment/staging/vehicle-data.csv "
        "> /home/project/airflow/dags/finalassignment/staging/csv_data.csv"
    ),
    dag=dag,
)

# define the third task
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=(
        "awk -F',' '{print $1, $2, $3, $4}' "
        "/home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv "
        "> /home/project/airflow/dags/finalassignment/staging/tsv_data.csv"
    ),
    dag=dag,
)

# define the fourth task
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=(
        "awk -F',' '{print $1, $2, $3, $4}' "
        "/home/project/airflow/dags/finalassignment/staging/payment-data.txt "
        "> /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv"
    ),
    dag=dag,
)

# define the fifth task
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=(
        "paste "
        "/home/project/airflow/dags/finalassignment/staging/csv_data.csv "
        "/home/project/airflow/dags/finalassignment/staging/tsv_data.csv "
        "/home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv "
        "> /home/project/airflow/dags/finalassignment/staging/extracted_data.csv"
    ),
    dag=dag,
)

# define the sixth task
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=(
        "awk -F',' '{OFS=\",\"; $5=toupper($5); print}' "
        "/home/project/airflow/dags/finalassignment/staging/extracted_data.csv "
        "> /home/project/airflow/dags/finalassignment/staging/transformed_data.csv"
    ),
    dag=dag,
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
