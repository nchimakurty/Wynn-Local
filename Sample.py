from __future__ import annotations

import os
import json, logging
import pendulum
from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.sensors.python import PythonSensor
from include.extensions.operators.custom_mssqloperator import MsSqlOperatorXcom
from include.extensions.operators.custom_databricks_operators import DatabricksSubmitRunAuditLogOperatorAsync
from include import notification_functions
from airflow.models import DagRun
from airflow.hooks.base import BaseHook
from airflow.utils.email import send_email
import pandas as pd


logging.getLogger('azure.core').setLevel(logging.WARN)

ABC_SQL_CONN_ID = 'CIP_EDW_AUDIT_SQLDB_CONN'
DATABRCIKS_CONN_ID = 'CIP-EDW-DATABRICKS-CONN'

def check_dag_run_status(dag_id_to_check, state, ti):
    dag_run = DagRun.find(
        dag_id=dag_id_to_check,
        state=state
        )
    return not len(dag_run) > 0

def fetch_all_as_dict(cursor):
    columns = [column[0] for column in cursor.description]
    temp_results =  [dict(zip(columns, row)) for row in cursor.fetchall()]
    results = {}
    for cnt,elem in enumerate(temp_results):
        row_key = elem.get('TSK_NM',f'{cnt}')
        results.update({row_key : elem }) 
    return results

def get_abc_job_run_details(ti):
    data = ti.xcom_pull(task_ids='fetch_abc_metadata', key='return_value')
    if type(data) != dict:
        raise Exception('Invalid type found for ABC metadata')
    if len(list(data.values())) == 0:
        raise Exception('No metadata from ABC found for the current run')
    data = list(data.values())[2]
    try:
        data['TSK_CNFGRTN'] = json.loads(data['TSK_CNFGRTN'])
    except:
        pass
    return data

def get_abc_job_run_details_databricks(ti):
    data = ti.xcom_pull(task_ids='fetch_abc_metadata', key='return_value')
    if type(data) != dict:
        raise Exception('Invalid type found for ABC metadata')
    if len(list(data.values())) == 0:
        raise Exception('No metadata from ABC found for the current run')
    databricks_tasks = {}
    for key, value in data.items():
        if value['TSK_CNFGRTN'] is not None and "notebook_task" in value['TSK_CNFGRTN']:
            databricks_tasks.update({key:value})
            databricks_tasks[key]['TSK_CNFGRTN'] = json.loads(value['TSK_CNFGRTN'])
            databricks_tasks[key]['TSK_CNFGRTN']['notebook_task']['base_parameters'].update({'JOB_RUN_ID':value['JOB_RUN_ID']})
            databricks_tasks[key]['TSK_CNFGRTN']['notebook_task']['base_parameters'].update({'PRCSSNG_RNGE_STRT_DT_TM':value['PRCSSNG_RNGE_STRT_DT_TM']})
    
    return databricks_tasks

def cipedw_operawlv_hotelguestdailyagg_load_report_to_html_email(ti, **kwargs):    
    sql_file_path = '/usr/local/airflow/include/sql/snowflake/bkf/hotelguestdailyagg_load_status_check.sql'    
   
    with open(sql_file_path, 'r') as file:
        query = file.read()
    
    hook = BaseHook.get_hook('CIP_EDW_AUDIT_SQLDB_CONN')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute(query)
    rows = cursor.fetchall()

    if rows:
        print("dag_cipedw_operawlv_hotelguestdailyagg_load completed before BKF SLA. Skipping email.")
        return

    today_str = datetime.today().strftime('%Y-%m-%d')
    cip_env = os.getenv("AIRFLOW__WEBSERVER__INSTANCE_NAME", default=None)
    email_to = ti.xcom_pull(task_ids='get_abc_curr_job_run_details', key='return_value')['TSK_CNFGRTN']['to']
    email_cc = ti.xcom_pull(task_ids='get_abc_curr_job_run_details', key='return_value')['TSK_CNFGRTN']['cc']
    email_subject = 'Hotelguestdailyagg Data Refresh Delay Report - BKF Promo'
    email_html_content = f'''
    <p>Hi All,</p>
    <p>Please note that Hotelguestdailyagg Data Refresh for WLV has not been completed by 7:30 AM PT today. Please inform business about possible missing BKF reservations for BKF today.</p>
    <p>Environment: {os.getenv("AIRFLOW__WEBSERVER__INSTANCE_NAME", default="N/A")}</p>
    <p>Thanks,<br>CIP Team</p>
    '''
    
    send_email(
        to=email_to,
        cc=email_cc,
        subject=email_subject,
        html_content=email_html_content
    )

    cursor.close()
    conn.close()


def bkf_reservervations_stats_report_to_html_email(ti, **kwargs):    
    sql_file_path = '/usr/local/airflow/include/sql/snowflake/bkf/bkf_reservervations_stats.sql'
   
    with open(sql_file_path, 'r') as file:
        query = file.read()
    
    hook = BaseHook.get_hook('CIP_EDW_SNOWFLAKE_CONN')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute(query)
    rows = cursor.fetchall()

    success_count = 0
    error_count = 0

    columns = [desc[0] for desc in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    for row in rows:
        status = row.get('BKF_RESERVATION_CREATE_STATUS', '')
        cnt = row.get('CNT', 0)
        if status == 'SUCCESS':
            success_count = cnt
        elif status == 'ERROR':
            error_count = cnt
    
    total_count = success_count + error_count

    today_str = datetime.today().strftime('%Y-%m-%d')
    cip_env = os.getenv("AIRFLOW__WEBSERVER__INSTANCE_NAME", default=None)
    
    email_subject = f'BKF Reservations Stats for {today_str}'
    email_html_content = f'''
    <p>Hi All,</p>
    <p>Please find the BKF Reservations Stats for {today_str}.</p>
    <p>Total BKF promo eligible reservations: {total_count}.</p>
    <p>Success Count: {success_count} / {total_count}.</p>
    <p>Error Count: {error_count} / {total_count}.</p>
    <p>Environment: {os.getenv("AIRFLOW__WEBSERVER__INSTANCE_NAME", default="N/A")}</p>
    <p>Thanks,<br>CIP Team</p>
    '''
    
    email_to = ti.xcom_pull(task_ids='get_abc_curr_job_run_details', key='return_value')['TSK_CNFGRTN']['to']
    email_cc = ti.xcom_pull(task_ids='get_abc_curr_job_run_details', key='return_value')['TSK_CNFGRTN']['cc']

    send_email(
        to=email_to,
        cc=email_cc,
        subject=email_subject,
        html_content=email_html_content
    )

    cursor.close()
    conn.close()


#Input dataset
parent__dag_cipedw_operawlv_hotelguestdailyagg_load_dataset = Dataset('snowflake://dag_cipedw_operawlv_hotelguestdailyagg_load__dag_bkfbuffetreservation_load')

with DAG(
    dag_id='dag_bkfbuffetreservation_load',
    description="This dag ingests all the reservations with eligible rate code for BKF Promo and keep track of updates in the reservation.",
    start_date=pendulum.datetime(2023, 3, 7, tz='America/Los_Angeles'),
    end_date=None,
        schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("30 7 * * *", timezone="America/Los_Angeles"),
        datasets=[parent__dag_cipedw_operawlv_hotelguestdailyagg_load_dataset]
    ),
    default_args={
        'retries':3,
        'retry_delay':timedelta(minutes=5),
        "on_failure_callback": notification_functions.failure_callback,
        "on_retry_callback": notification_functions.retry_callback
    },
    tags=['EDW-JOB', 'CIPEDW','DINING','BKF','SEVENROOMS'],
    render_template_as_native_obj=True,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=4,
    catchup=False,
) as dag:

    fetch_abc_metadata = MsSqlOperatorXcom(
        task_id='fetch_abc_metadata',
        mssql_conn_id=ABC_SQL_CONN_ID,
        sql="EXECUTE [db_cipabc].[sp_get_abc_data] @JOB_NM='{{dag.dag_id}}'",
        handler=fetch_all_as_dict
    )


    get_abc_curr_job_run_details = PythonOperator(
        task_id='get_abc_curr_job_run_details',
        python_callable=get_abc_job_run_details
    )


    get_abc_curr_job_run_details_databricks = PythonOperator(
        task_id='get_abc_curr_job_run_details_databricks',
        python_callable=get_abc_job_run_details_databricks
    )

    cipedw_operawlv_hotelguestdailyagg_load_report = PythonOperator(
        task_id='cipedw_operawlv_hotelguestdailyagg_load_report',
        python_callable=cipedw_operawlv_hotelguestdailyagg_load_report_to_html_email,
        provide_context=True
    )

    check_dag_cipedw_operawlv_hotelguestdailyagg_load_sensor = PythonSensor(
        task_id='check_dag_cipedw_operawlv_hotelguestdailyagg_load_sensor',
        mode='reschedule',
        poke_interval=300,
        timeout=7200,
        python_callable=check_dag_run_status,
        op_kwargs={'dag_id_to_check': "dag_cipedw_operawlv_hotelguestdailyagg_load", "state" : "running"},
        )

    begin_audit_job_execution = MsSqlOperator(
        task_id='begin_audit_job_execution',
        mssql_conn_id=ABC_SQL_CONN_ID,
        sql='''
            EXECUTE [db_cipabc].[sp_log_abc_data]
             @JOB_MSTR_ID={{ ti.xcom_pull(task_ids='get_abc_curr_job_run_details', key='return_value')['JOB_MSTR_ID'] }}
            ,@JOB_RUN_ID={{ ti.xcom_pull(task_ids='get_abc_curr_job_run_details', key='return_value')['JOB_RUN_ID'] }}
            ,@EXCTN_DT='{{ ti.xcom_pull(task_ids='get_abc_curr_job_run_details', key='return_value')['EXCTN_DT'] }}'
            ,@LOG_TYPE='JOB EXECUTION'
            ,@LOG_STATUS='RUNNING'
            ,@DML_ACTION='INSERT'
           '''         
           )


    bkfbuffetreservation_data_load = DatabricksSubmitRunAuditLogOperatorAsync(
           task_id="bkfbuffetreservation_data_load",
           retries=3,
           execution_timeout=timedelta(hours=2),
           databricks_conn_id=DATABRCIKS_CONN_ID,
           new_cluster='{{ ti.xcom_pull(task_ids="get_abc_curr_job_run_details_databricks")["bkfbuffetreservation_data_load"]["TSK_CNFGRTN"]["new_cluster"] }}',
           notebook_task='{{ ti.xcom_pull(task_ids="get_abc_curr_job_run_details_databricks")["bkfbuffetreservation_data_load"]["TSK_CNFGRTN"]["notebook_task"] }}',
           polling_period_seconds='{{ ti.xcom_pull(task_ids="get_abc_curr_job_run_details_databricks")["bkfbuffetreservation_data_load"]["TSK_CNFGRTN"]["polling_period_seconds"] }}',           
           mssql_conn_id=ABC_SQL_CONN_ID,
           should_log_to_abc=True,
           run_name="bkfbuffetreservation_data_load",
           )



    on_ingestion_success = EmptyOperator(task_id='on_ingestion_success',trigger_rule='all_done',)       

    bkf_reservervations_stats_report = PythonOperator(
        task_id='bkf_reservervations_stats_report',
        python_callable=bkf_reservervations_stats_report_to_html_email,
        provide_context=True
    )

    end_audit_job_execution = MsSqlOperator(
        task_id='end_audit_job_execution',
        mssql_conn_id=ABC_SQL_CONN_ID,
        trigger_rule='all_done',
        sql='''
        
                   EXECUTE [db_cipabc].[sp_log_abc_data]
                   @JOB_MSTR_ID={{ ti.xcom_pull(task_ids='get_abc_curr_job_run_details', key='return_value')['JOB_MSTR_ID'] }}
                  ,@JOB_RUN_ID={{ ti.xcom_pull(task_ids='get_abc_curr_job_run_details', key='return_value')['JOB_RUN_ID'] }}
                  ,@EXCTN_DT='{{ ti.xcom_pull(task_ids='get_abc_curr_job_run_details', key='return_value')['EXCTN_DT'] }}'
                  ,@LOG_TYPE='JOB EXECUTION'
                  ,@LOG_STATUS='SUCCESS'
                  ,@DML_ACTION='UPDATE'
           '''       
    )
    
    mark_job_ready_for_nextrun = MsSqlOperator(
        task_id='mark_job_ready_for_nextrun',
        mssql_conn_id="CIP_EDW_AUDIT_SQLDB_CONN",
        trigger_rule='all_success',
        sql='''
        
                   EXECUTE [db_cipabc].[sp_log_abc_data]
                   @JOB_MSTR_ID={{ ti.xcom_pull(task_ids='get_abc_curr_job_run_details', key='return_value')['JOB_MSTR_ID'] }}
                  ,@DML_ACTION='INSERT'
                  ,@LOG_TYPE='JOB CONTROL'
        '''       
        )



    fetch_abc_metadata >> get_abc_curr_job_run_details >> get_abc_curr_job_run_details_databricks >> cipedw_operawlv_hotelguestdailyagg_load_report >> check_dag_cipedw_operawlv_hotelguestdailyagg_load_sensor >> begin_audit_job_execution >> bkfbuffetreservation_data_load >> on_ingestion_success >> bkf_reservervations_stats_report >> end_audit_job_execution >> mark_job_ready_for_nextrun
