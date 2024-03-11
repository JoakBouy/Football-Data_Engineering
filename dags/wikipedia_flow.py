from airflow import DAG

dag = DAG(
    dag_id='wikipedia_flow',
    default_args={
        'owner': 'Buoy Gai',
        'start_date': datetime(year=2021, month=3, day=12),
    },
    schedule_interval=None,
    catchup=False
)

#Extraction
#Preprocessing
#write to database
    