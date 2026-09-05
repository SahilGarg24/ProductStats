from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lower, col
import requests, json, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Start Spark session
spark = SparkSession.builder.appName("ProductStatsData").getOrCreate()

o9Customer = "URBN"
Tenant = "EKGDeploymentTest3-Prod"
sftp_folder = "T1"
path = "gs://o9dl-gcpgs10054"
ssh_username = 'etluser@EKGDeploymentTest3.com'
ssh_password = 'Welcome@1234'
tenant_url = "https://mygcppmm.o9solutions.com"

# Process the Solution string into a cleaned, lowercase list + 'Common'
Solution = "Supply Planning, Demand Planning"
Solution = Solution.split(",")                            # Split by comma
Solution = [item.strip() for item in Solution]            # Trim spaces
Solution = [s.lower() for s in Solution]                  # Convert to lowercase
Solution.append("common")                                 # Add 'Common'

# Step 1: Auth - Get token
payload = {
    "userEmail": ssh_username,
    "submittedPassword": ssh_password,
    "expirySeconds": 36000
}

headers = {
    "Content-Type": "application/json"
}

token_url = tenant_url + "/api/framework/auth/login"

response = requests.request("POST", token_url, headers=headers, data=json.dumps(payload), verify=False)
print(response)

key = response.json()["AuthToken"]
api_key = 'Basic {0}'.format(key)
print('api_key: {0}'.format(api_key))

# Step 2: Get workspace_id
payload = {}
headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': api_key
}

wid_url = '{0}/api/v3/integration/{1}/Workspaces'.format(tenant_url, Tenant.split("-")[0])
response = requests.request("GET", wid_url, headers=headers, data=json.dumps(payload), verify=False)
workspace_id = response.json()[0]["Id"]

# Step 3: List files from DataLake
url = "{0}/api/v3/integration/{1}/workspaces/{2}/DataLakeFiles?pathPrefix=CCM".format(tenant_url, Tenant.split("-")[0], workspace_id)

payload = {}
headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': api_key
}

response = requests.request("GET", url, headers=headers, data=payload)
print(response)

# Generating the post characters for file naming
PostFileName = o9Customer + "_" + sftp_folder
print(PostFileName)

schema = StructType([
    StructField("Version.[Version Name]", StringType(), True),
    StructField("Sequence ID.[Sequence ID]", StringType(), True),
    StructField("Tenant Details.[Tenant]", StringType(), True),
    StructField("Model Stats Member", StringType(), True),
    StructField("Model Stats Input Query", StringType(), True)
])

df = spark.createDataFrame([], schema)

# Add seq_id and collect to driver for row-wise processing
seq_id = 1
rows = []
queries = input_001

for row in queries.filter(lower(col("solution")).isin(Solution)).collect():
    rows.append((
        "CurrentWorkingView",
        str(seq_id).zfill(4),
        Tenant,
        row["member"],
        row["query"]
    ))
    seq_id += 1

# Create new DataFrame from the collected rows
output_001 = spark.createDataFrame(rows, schema=schema)
