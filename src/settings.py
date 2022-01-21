import boto3
import os

env = os.environ['ENVIRONMENT']
proj = os.environ['PROJECTNAME']
ssm = boto3.client("ssm")

ssmbase = f"/app/{proj}/lambda/{env}"
ELQ_USER = ssm.get_parameter(Name=f"{ssmbase}/Lyyti_ELQ_USER")["Parameter"]["Value"]
ELQ_PASSWORD = ssm.get_parameter(Name=f"{ssmbase}/Lyyti_ELQ_PASSWORD", WithDecryption=True)["Parameter"]["Value"]
ELQ_BASE_URL = ssm.get_parameter(Name=f"{ssmbase}/Lyyti_ELQ_BASE_URL")["Parameter"]["Value"]

PSQL_DB_NAME = ssm.get_parameter(Name=f"{ssmbase}/PSQL_DB_NAME")["Parameter"]["Value"]
PSQL_DB_USER = ssm.get_parameter(Name=f"{ssmbase}/PSQL_DB_USER")["Parameter"]["Value"]
PSQL_DB_PASSWORD = ssm.get_parameter(Name=f"{ssmbase}/PSQL_DB_PASSWORD", WithDecryption=True)["Parameter"]["Value"]
PSQL_DB_HOST_NAME = ssm.get_parameter(Name=f"{ssmbase}/PSQL_DB_HOST_NAME")["Parameter"]["Value"]
PSQL_DB_URI = "postgresql+psycopg2://" + PSQL_DB_USER + ":" + PSQL_DB_PASSWORD + "@" + PSQL_DB_HOST_NAME + "/" + PSQL_DB_NAME