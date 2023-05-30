from pathlib import Path
from airflow.utils.dates import days_ago
from os.path import join
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from operators.twitter_operator import TwitterOperator
from datetime import datetime, timedelta
from airflow.models import DAG
from pathlib import Path
import sys
sys.path.append("airflow_pipeline")


with DAG(dag_id="TwitterDAG", start_date=days_ago(6), schedule_interval="@daily") as dag:
    BASE_FOLDER = join(
        str(Path("~/Documents").expanduser()),
        "/home/raf/pipeline-transform/datalake/{stage}/twitter_datascience/{partition}",
    )
    PARTITION_FOLDER_EXTRACT = "extract_date={{ data_interval_start.strftime('%Y-%m-%d') }}"

    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    query = "datascience"

    twitter_operator = TwitterOperator(file_path=join(BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER_EXTRACT),
                                                      "datascience_{{ ds_nodash }}.json"),
                                       query=query,
                                       start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                                       end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                                       task_id="twitter_datascience")

    twitter_transform = SparkSubmitOperator(task_id="transform_twitter_datascience",
                                            application="/home/raf/pipeline-transform/src/spark/transformation.py",
                                            name="twitter_transformation",
                                            application_args=["--src", BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER_EXTRACT),
                                                              "--dest", BASE_FOLDER.format(
                                                                  stage="silver", partition=PARTITION_FOLDER_EXTRACT),
                                                              "--process-date", "{{ ds }}"])

    twitter_insight = SparkSubmitOperator(task_id="insight_twitter",
                                          application="/home/raf/pipeline-transform/src/spark/insight_tweet.py",
                                          name="insight_twitter",
                                          application_args=["--src", BASE_FOLDER.format(stage="silver", partition=""),
                                                            "--dest", BASE_FOLDER.format(
                                                                stage="gold", partition=""),
                                                            "--process-date", "{{ ds }}"])


twitter_operator >> twitter_transform >> twitter_insight
