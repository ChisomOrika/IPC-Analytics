import os

CSV_FILE_DIR = os.getenv("CSV_FILE_DIR", '/opt/airflow/dags/datasets/loyverse/')
CSV_FILE_DIR_LENCO = os.getenv("CSV_FILE_DIR_LENCO", '/opt/airflow/dags/datasets/lenco/')
loyverse_url = os.getenv("loyverse_url",'https://api.loyverse.com/v1.0/')
loyverse_parameter = os.getenv("loyverse_parameter",{'Authorization': 'Bearer e0a091eed67b4174bc839ada43a49433'})
lenco_url = os.getenv("lenco_url",'https://api.lenco.ng/access/v1/')
lenco_headers = os.getenv("lenco_headers",{
    "accept": "application/json",
    "Authorization": 'Bearer 255bee173ae8e711e92384574b1d46e4e74d4248143246fad7c6985e9ab3c3d9'
})
CSV_FILE_DIR_TEST = os.getenv("CSV_FILE_DIR_TEST", 'C:/Users/chiso/airflow/dags/datasets/loyverse/')

PSQL_DB = os.getenv("PSQL_DB", "ipc")
PSQL_USER = os.getenv("PSQL_USER", "postgres")
PSQL_PASSWORD = os.getenv("PSQL_PASSWORD", "Chisom33")
PSQL_PORT =  "5432"
PSQL_HOST = "host.docker.internal"
PSQL_HOST_LH = "localhost"
sqlalchemy_connection_string_lh = "postgresql://postgres:Chisom33@localhost:5432/ipc"
sqlalchemy_connection_string_docker = "postgresql://postgres:Chisom33@host.docker.internal:5432/ipc"
