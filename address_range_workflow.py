import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def checkStatus(context):
    dag_run = context.get("dag_run")
    failed_zones = []
    logging.info(f"all zones {context['params']}")
    all_zones = context["params"]["country"]
    # get all failed task instances
    failed_tasks = dag_run.get_task_instances(state="failed")
    for task in failed_tasks:
        failed_task = task.task_id
        # check only for trigger_change_generation_workflow and expanded inputs
        if task.map_index != -1 and (
                task.task_id == orbis_address_ranges_task.task_id or task.task_id == genesis_address_ranges_task.task_id):
            failed_zones.append(all_zones[task.map_index])

    logging.info(f"failed task: {failed_tasks}, failed zones: {failed_zones}")
    # return {
    #     "failed_task": failed_tasks,
    #     "failed_zones": failed_zones,
    # }


# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
dag = DAG(
    'address_range_workflow',
    default_args=default_args,
    description='Address Range Workflow',
    schedule=None,

    params={
        "country": ["BEL", "BGR", "CHE", "CYP", "CZE", "DEU", "DNK", "ESP", "EST", "FIN", "FRA",
                    "GBR", "GRC", "HRV", "HUN", "IRL", "ITA", "LTU", "LUX", "LVA", "MLT", "POL",
                    "PRT", "ROU", "SVK", "SVN", "SWE", "AUT", "BRA", "NLD"],

        "week": '03_004',
        "OrbisVenturaRelease": '24090.000',
        "OrbisFileName": 'Orbis_24090.000',
        "GenesisFileName": 'Genesyis2024_week_03_007'

    }
)


def build_zone_wise_notebook_params(**context):
    params = context["params"]
    country_list = params["country"]
    zone_wise_params = []
    for country in country_list:
        zone_wise_params.append({"country": country,
                                 "week": params["week"],
                                 "OrbisVenturaRelease": params["OrbisVenturaRelease"],
                                 "OrbisFileName": params["OrbisFileName"],
                                 "GenesisFileName": params["GenesisFileName"]
                                 })
    return zone_wise_params

def check_status(**context):
    dag_run = context.get("dag_run")
    failed_zones = []
    logging.info(f"all zones {context['params']}")
    all_zones = context["params"]["country"]
    # get all failed task instances
    failed_tasks = dag_run.get_task_instances(state="failed")
    for task in failed_tasks:
        # check only for trigger_change_generation_workflow and expanded inputs
        if task.map_index != -1 and (
                task.task_id == orbis_address_ranges_task.task_id or task.task_id == genesis_address_ranges_task.task_id):
            failed_zones.append(all_zones[task.map_index])
    logging.info(f"failed task: {failed_tasks}, failed zones: {failed_zones}")

build_zone_wise_input = PythonOperator(
    task_id='build_zone_wise_input',
    python_callable=build_zone_wise_notebook_params,
    provide_context=True,
    dag=dag
)

# Define the notebook task Genesis
genesis_address_ranges_task = DatabricksRunNowOperator.partial(
    task_id='genesis_address_ranges_task',
    databricks_conn_id='databricks_default',  # Connection ID to Databricks
    job_id="806933263803455",  # Job ID of the notebook
    dag=dag,
    trigger_rule="all_done"
).expand(notebook_params=build_zone_wise_input.output)

# Define the notebook task Orbis
orbis_address_ranges_task = DatabricksRunNowOperator.partial(
    task_id='orbis_address_ranges_task',
    databricks_conn_id='databricks_default',  # Connection ID to Databricks
    job_id="827871496486372",  # Job ID of the notebook
    dag=dag,
    trigger_rule="all_done"
).expand(notebook_params=build_zone_wise_input.output)

# Define the notebook task Orbis
statisticsGenerationGenesisOrbis_task = DatabricksRunNowOperator(
    task_id='statisticsGenerationGenesisOrbis_task',
    databricks_conn_id='databricks_default',  # Connection ID to Databricks
    job_id="822099345857462",  # Job ID of the notebook
    dag=dag,
    notebook_params={"OrbisFileName": "{{params.OrbisFileName}}",
                     "GenesisFileName": "{{params.GenesisFileName}}"},
    trigger_rule="all_done"

)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=check_status,
    provide_context=True,
    dag=dag,
    trigger_rule="all_done"
)

# Set task dependencies
build_zone_wise_input >> genesis_address_ranges_task >> orbis_address_ranges_task >> statisticsGenerationGenesisOrbis_task >> end_task
