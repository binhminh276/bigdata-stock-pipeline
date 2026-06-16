from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'hadoop',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bigdata_stock_pipeline_15h15',
    default_args=default_args,
    description='Luồng xử lý chứng khoán - Lượt chiều 15h15',
    schedule='15 8 * * 1-5',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stock_project', 'bigdata'],
) as dag:

    sqoop_import_node = BashOperator(
        task_id='sqoop_import_node',
        bash_command='/home/hadoop/sqoop_import_stock.sh ',
    )

    hive_clean_node = BashOperator(
        task_id='hive_clean_node',
        bash_command='hive -f /home/hadoop/clean_data_advanced.hql',
    )

    pyspark_join_kafka_node = BashOperator(
        task_id='pyspark_join_kafka_node',
        bash_command='spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 /home/hadoop/bigdata_spark/spark_master_pipeline.py ',
    )

    # ĐỊNH NGHĨA SPARK THEO TỪNG CỤM ANH EM CA CHIỀU (MỖI NGƯỜI 2 FILE)
    spark_bao_1 = BashOperator(task_id='node3_spark_bao_task51', bash_command='spark-submit /home/hadoop/bigdata_spark/spark_bao_task51.py ')
    spark_bao_2 = BashOperator(task_id='node3_spark_bao_task510', bash_command='spark-submit /home/hadoop/bigdata_spark/spark_bao_task510.py ')

    spark_minh_1 = BashOperator(task_id='node3_spark_minh_task52', bash_command='spark-submit /home/hadoop/bigdata_spark/spark_minh_task52.py ')
    spark_minh_2 = BashOperator(task_id='node3_spark_minh_task59', bash_command='spark-submit /home/hadoop/bigdata_spark/spark_minh_task59.py ')

    spark_an_1 = BashOperator(task_id='node3_spark_an_task53', bash_command='spark-submit /home/hadoop/bigdata_spark/spark_an_task53.py ')
    spark_an_2 = BashOperator(task_id='node3_spark_an_task58', bash_command='spark-submit /home/hadoop/bigdata_spark/spark_an_task58.py ')

    spark_thuong_1 = BashOperator(task_id='node3_spark_thuong_task54', bash_command='spark-submit /home/hadoop/bigdata_spark/spark_thuong_task54.py ')
    spark_thuong_2 = BashOperator(task_id='node3_spark_thuong_task55', bash_command='spark-submit /home/hadoop/bigdata_spark/spark_thuong_task55.py ')

    spark_duy_1 = BashOperator(task_id='node3_spark_duy_task56', bash_command='spark-submit /home/hadoop/bigdata_spark/spark_duy_task56.py ')
    spark_duy_2 = BashOperator(task_id='node3_spark_duy_task57', bash_command='spark-submit /home/hadoop/bigdata_spark/spark_duy_task57.py ')

    # THIẾT LẬP LUỒNG CA CHIỀU
    sqoop_import_node >> hive_clean_node

    hive_clean_node >> [spark_bao_1, spark_bao_2]

    [spark_bao_1, spark_bao_2] >>  spark_minh_1
    [spark_bao_1, spark_bao_2] >>  spark_minh_2

    [spark_minh_1, spark_minh_2] >> spark_an_1
    [spark_minh_1, spark_minh_2] >> spark_an_2

    [spark_an_1, spark_an_2] >> spark_thuong_1
    [spark_an_1, spark_an_2] >> spark_thuong_2

    [spark_thuong_1, spark_thuong_2] >> spark_duy_1
    [spark_thuong_1, spark_thuong_2] >> spark_duy_2

    [spark_duy_1, spark_duy_2] >> pyspark_join_kafka_node