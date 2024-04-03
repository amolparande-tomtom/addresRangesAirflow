import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

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
    'address_range_pipeline',
    default_args=default_args,
    description='Address Range Pipeline',
    schedule=None,

    params={
        "country": ["BEL", "BGR", "CHE", "CYP", "CZE", "DEU", "DNK", "ESP", "EST", "FIN", "FRA",
                    "GBR", "GRC", "HRV", "HUN", "IRL", "ITA", "LTU", "LUX", "LVA", "MLT", "POL",
                    "PRT", "ROU", "SVK", "SVN", "SWE", "AUT", "BRA", "NLD"],

        "week": '03_004',
        "OrbisVenturaRelease": '24090.000',
        "OrbisFileName": 'Orbis_24090.000',
        "GenesisFileName": 'Genesis2024_week_03_007'

    }
)


def get_failed_genesis_ranges_zones(**context):
    dag_run = context.get("dag_run")
    failed_zones = []
    logging.info(f"all zones {context['params']}")
    all_zones = context["params"]["country"]
    # get all failed task instances
    failed_tasks = dag_run.get_task_instances(state="failed")
    for task in failed_tasks:
        # check only for genesis_address_ranges_task and expanded inputs
        if task.map_index != -1 and (task.task_id == genesis_address_ranges_task.task_id):
            failed_zones.append(all_zones[task.map_index])
    logging.info(f"failed genesis_address_ranges_task task: {failed_tasks}, failed zones: {failed_zones}")
    return failed_zones


# get_failed_orbis_ranges_zones

def get_completed_orbis_ranges_zones(**context):
    dag_run = context.get("dag_run")
    failed_zones = []
    logging.info(f"all zones {context['params']}")
    all_zones = context["params"]["country"]
    # get all failed task instances
    failed_tasks = dag_run.get_task_instances(state="failed")
    for task in failed_tasks:
        # check only for genesis_address_ranges_task and expanded inputs
        if task.map_index != -1 and (task.task_id == orbis_address_ranges_task.task_id):
            failed_zones.append(all_zones[task.map_index])
    logging.info(f"failed orbis_address_ranges_task task: {failed_tasks}, failed zones: {failed_zones}")
    params = context["params"]
    all_countries = params["country"]
    genesis_fail_zones = context["ti"].xcom_pull(task_ids="get_failed_genesis_ranges_zones")
    genesis_fail_zones = genesis_fail_zones if genesis_fail_zones else []
    completed_zone = []
    for country in all_countries:
        if country not in failed_zones and country not in genesis_fail_zones:
            completed_zone.append(country)

    logging.info(f'genesis and orbis done country list {completed_zone}')

    return completed_zone


def build_zone_wise_input_for_orbis(**context):
    failed_zones = context["ti"].xcom_pull(task_ids="get_failed_genesis_ranges_zones")
    failed_zones = failed_zones if failed_zones else []
    logging.info(f"failed_zones: {failed_zones}")
    params = context["params"]
    all_countries = params["country"]
    zone_wise_params = []
    for country in all_countries:
        if country not in failed_zones:
            zone_wise_params.append({"country": country,
                                     "week": params["week"],
                                     "OrbisVenturaRelease": params["OrbisVenturaRelease"],
                                     "OrbisFileName": params["OrbisFileName"],
                                     "GenesisFileName": params["GenesisFileName"]
                                     })

    return zone_wise_params


def build_zone_wise_input_for_genesis(**context):
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
    failed_orbis_task_zones = []
    failed_genesis_task_zones = []
    logging.info(f"all zones {context['params']}")
    all_zones = context["params"]["country"]
    # get all failed task instances
    all_failed_tasks = dag_run.get_task_instances(state="failed")
    for task in all_failed_tasks:
        # check only for trigger_change_generation_workflow and expanded inputs
        if task.map_index != -1:
            if task.task_id == genesis_address_ranges_task.task_id:
                failed_genesis_task_zones.append(all_zones[task.map_index])
            if task.task_id == orbis_address_ranges_task.task_id:
                failed_orbis_task_zones.append(all_zones[task.map_index])
    result = {
        "failed_genesis_task_zones": failed_genesis_task_zones,
        "failed_orbis_task_zones": failed_orbis_task_zones
    }
    logging.info(f"failed task: {all_failed_tasks}, failed zones: {result}")


build_zone_wise_input_for_genesis_task = PythonOperator(
    task_id="build_zone_wise_input_for_genesis",
    python_callable=build_zone_wise_input_for_genesis,
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
).expand(notebook_params=build_zone_wise_input_for_genesis_task.output)

build_zone_wise_input_for_orbis_task = PythonOperator(
    task_id='build_zone_wise_input_for_orbis',
    python_callable=build_zone_wise_input_for_orbis,
    provide_context=True,
    dag=dag,
    trigger_rule="all_done"
)

# Define the notebook task Orbis
orbis_address_ranges_task = DatabricksRunNowOperator.partial(
    task_id='orbis_address_ranges_task',
    databricks_conn_id='databricks_default',  # Connection ID to Databricks
    job_id="827871496486372",  # Job ID of the notebook
    dag=dag,
    trigger_rule="all_done"
).expand(notebook_params=build_zone_wise_input_for_orbis_task.output)

# Define the notebook task Orbis
statisticsGenerationGenesisOrbis_task = DatabricksRunNowOperator(
    task_id='statisticsGenerationGenesisOrbis_task',
    databricks_conn_id='databricks_default',  # Connection ID to Databricks
    job_id="822099345857462",  # Job ID of the notebook
    dag=dag,
    notebook_params={"OrbisFileName": "{{params.OrbisFileName}}",
                     "GenesisFileName": "{{params.GenesisFileName}}",
                     "SuccessfulCountryLists": "{{ task_instance.xcom_pull(task_ids='get_completed_orbis_ranges_zones_task') }}"},

    trigger_rule="all_done"

)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=check_status,
    provide_context=True,
    dag=dag,
    trigger_rule="all_done"
)

get_failed_genesis_ranges_zones_task = PythonOperator(
    task_id='get_failed_genesis_ranges_zones_task',
    python_callable=get_failed_genesis_ranges_zones,
    provide_context=True,
    dag=dag,
    trigger_rule="all_done"
)

get_completed_orbis_ranges_zones_task = PythonOperator(
    task_id='get_completed_orbis_ranges_zones_task',
    python_callable=get_completed_orbis_ranges_zones,
    provide_context=True,
    dag=dag,
    trigger_rule="all_done"
)

# Set task dependencies
build_zone_wise_input_for_genesis_task >> genesis_address_ranges_task >> get_failed_genesis_ranges_zones_task >> build_zone_wise_input_for_orbis_task
build_zone_wise_input_for_orbis_task >> orbis_address_ranges_task >> get_completed_orbis_ranges_zones_task >>statisticsGenerationGenesisOrbis_task >> end_task
