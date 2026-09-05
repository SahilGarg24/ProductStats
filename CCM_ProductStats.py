from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.helpers import chain
from airflow.models import Variable
from airflow import DAG
from io import BytesIO
import paramiko
import json
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator, BranchPythonOperator
import csv
import pandas as pd
from datetime import datetime

# Specify the path to your JSON file
file_path = "/opt/airflow/dags/CCM_Parameters.json"
# Open the JSON file and load its contents
with open(file_path, "r") as file:
    Parameters = json.load(file)
#-----------------------------------------------------------------------------------------------------------------------
tenant_id = Parameters['Tenant_ID']
LS_URL = Parameters['LS_URL'] + '/api'
userEmail = Parameters['EtlUsername']
submittedPassword = Parameters['EtlPassword']
TenantName = Parameters['TenantName']
Customer = Parameters['CustomerName']
EnviName = Parameters['EnvironmentName']
sftp_folder_name = Parameters['SFTP_FolderName']
sftppassword = Parameters['SFTP_Password']
sftpusername = Parameters['SFTP_username']
flag = Parameters['Flag']
basepath = Parameters["BasePath"]
Solution = Parameters['Solutions'].split(",")
Solution = [item.strip() for item in Solution]
Solution = [s.lower() for s in Solution]
Solution.append("Common")

# Specify the path to your csv file
file_path = "{0}CCM/queries.csv".format(basepath)
queries = pd.read_csv(file_path, quotechar='"', quoting=csv.QUOTE_ALL, sep="^")
#-----------------------------------------------------------------------------------------------------------------------
# Create an empty DataFrame
df = pd.DataFrame(columns=[
    "Version.[Version Name]",
    "Sequence ID.[Sequence ID]",
    "Tenant Details.[Tenant]",
    "Model Stats Member",
    "Model Stats Input Query"
])

def get_product_stats():
    seq_id = 1
    j = 0
    for index, row in queries[queries["solution"].str.lower().isin(Solution)].iterrows():
        df.loc[j] = [
            "CurrentWorkingView", str(seq_id).zfill(4), str(TenantName), str(row["member"]), str(row["query"])]
        j += 1
        seq_id += 1

    try:
        filename = 'ProductStats_{0}_{1}.csv'.format(Customer, sftp_folder_name)
        remote_path = "/{0}/ToCCM/{1}".format(sftp_folder_name, filename)
        # Convert DataFrame to CSV in memory (bytes)
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False, sep="^")
        csv_buffer.seek(0)  # Go back to start of buffer
        # Initialize the SFTP client
        transport = paramiko.Transport(('sftpdcxprod.o9solutions.com', 2222))
        transport.connect(username=sftpusername, password=sftppassword)

        sftp = paramiko.SFTPClient.from_transport(transport)
        # Write buffer contents to remote SFTP file
        with sftp.file(remote_path, 'wb') as remote_file:
            remote_file.write(csv_buffer.read())

        print(f"CSV uploaded successfully to {remote_path}")
        # Close SFTP connection
        sftp.close()
        transport.close()

    except Exception as e:
        print(f"Error uploading to SFTP: {e}")

params = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    "o9AF_tenant_id": tenant_id,
    "o9AF_tenant_name": TenantName.split("-")[0],
    "o9AF_environment_name": EnviName.split("-")[0],
    "o9AF_webapi_url": LS_URL
}

with DAG('CCM_ProductStats',
         start_date=datetime(2024, 1, 1),
         max_active_runs=1,
         schedule_interval=None,
         params=params,
         catchup=False
         ) as dag:
    # Dummy start operator
    start = DummyOperator(task_id='Start', dag=dag)

    getproductstats_task = PythonOperator(
        task_id='Fetch_ProductStats',
        python_callable=get_product_stats,
        dag=dag
    )

    # Dummy End Operator
    end = DummyOperator(task_id='end')

    start >> getproductstats_task >> end
