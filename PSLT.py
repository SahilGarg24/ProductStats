import pandas as pd
import csv
import argparse

parser = argparse.ArgumentParser(description="Process Tenant and Solution arguments.")
# Define expected arguments
parser.add_argument("o9Customer", type=str, help="o9 Customer")
parser.add_argument("Tenant", type=str, help="Name of the Tenant")
parser.add_argument("Solution", type=str, help="Name of the Solution")
parser.add_argument("sftp_folder", type=str, help="SFTP Folder Name")
parser.add_argument("path", type=str, help="path")
# Parse the arguments
args = parser.parse_args()
# Access arguments
o9Customer = args.o9Customer
Tenant = args.Tenant
sftp_folder = args.sftp_folder
Solution = args.Solution
path = args.path
Solution = Solution.split(",")
Solution = [item.strip() for item in Solution]
Solution = [s.lower() for s in Solution]
Solution.append("Common")
queries = pd.read_csv(path, quotechar='"', quoting=csv.QUOTE_ALL, sep="^")

# Create an empty DataFrame
df = pd.DataFrame(columns=[
    "Version.[Version Name]",
    "Sequence ID.[Sequence ID]",
    "Tenant Details.[Tenant]",
    "Model Stats Member",
    "Model Stats Input Query"
])

seq_id = 1
j = 0
for index, row in queries[queries["solution"].str.lower().isin(Solution)].iterrows():
    df.loc[j] = [
        "CurrentWorkingView", str(seq_id).zfill(4), str(Tenant), str(row["member"]), str(row["query"])]
    j += 1
    seq_id += 1

df.to_csv("ProductStats_{0}_{1}.csv".format(o9Customer, sftp_folder), index=False, quotechar='"', quoting=csv.QUOTE_ALL, sep="^")

# seq_id = 1
# j = 0
# for index, row in queries[queries["solution"].isin(Solution)].iterrows():
#     df.loc[j] = [
#         "CurrentWorkingView", str(seq_id).zfill(4), str(Tenant), str(row["member"]), str(row["query"])]
#     j += 1
#     seq_id += 1
#
# df.to_csv("ProductStats_{0}_{1}.csv".format(o9Customer, sftp_folder), index=False, quotechar='"', quoting=csv.QUOTE_ALL, sep="^")
