# CREATE TABLE table_1_reports_metatada (
#     id SERIAL PRIMARY KEY,
#     doc_number VARCHAR(100),
#     doc_type VARCHAR(100),
#     title TEXT,
#     investment_code VARCHAR(100),
#     subject TEXT,
#     description TEXT,
#     entity TEXT,
#     ubigeo TEXT,
#     amount TEXT,
#     issue_date TEXT,
#     organic_unit TEXT,
#     path TEXT,
#     batch_id INTEGER
# );
import os, psycopg2, hashlib, json, logging
from tqdm import tqdm
from glob import glob
import pandas as pd

# setting up paths
LOGS_FOLDER = "./logs"
LOGNAME = f"/table_1_writer.log"
JSON_FILE = "paths.json"

# reading main parameters
csv_tables_file = open(JSON_FILE, "r")
csv_tables_dict = json.load(csv_tables_file)

# setting up logging
logging.basicConfig(filename=LOGS_FOLDER + LOGNAME,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)


def create_connection(database="complaints_project"):
    """
    Creates a cursor object to interact with the database
    """

    logging.info("create_connection: creating connection to database")
    
    connection = psycopg2.connect(
        user= os.environ["CGR_SQL_USER"],
        password= os.environ["CGR_SQL_PASS"],
        host = os.environ["CGR_SQL_HOST"],
        port="5432",
        database=database)
    
    logging.info("create_connection: connection successful")

    return connection


def generate_insert(fields_list, values_list, target_table="table_1_reports_metatada"):
    """
    Creates an insert statement for a given table and values
    """

    logging.info(f"generate_insert: creating insert statement for {target_table}")

    # updating value of fields_list if string
    values_string = ",".join(
            [f"'{str(mod_val)}'" if (type(mod_val) is str) else f"{str(mod_val)}" for mod_val in values_list]
        )

    # create the insert statement as a string
    # NOTE: items in fields_list are formatted to be commma-separated
    insert_statement = f"INSERT INTO {target_table} ({','.join(fields_list)}) VALUES ({values_string});"

    logging.info(f"generate_insert: successful insert statement creation")

    return insert_statement


def process_item(item: dict, conn: object):
    """
    Writes an item of data to the specified table
    """

    logging.info(f"process_item: writing item to table_1_reports_metatada")

    fields_list = [i for i in item.keys()]
    values_list = [item[i] for i in fields_list]

    insert_statement = generate_insert(fields_list, values_list)
    
    cursor = conn.cursor()
    cursor.execute(insert_statement)
    conn.commit()

    logging.info(f"process_item: item successfully writing")


def write_file_to_psql(csv_path: str, batch_id: int, conn: object):
    """
    Reads a csv file and writes the data to the specified table
    """

    logging.info(f"write_file_to_psql: writing to table_1_reports_metatada {csv_path}")

    # read csv file
    df = pd.read_csv(csv_path, delimiter="|")
    vars_vals = df.to_dict(orient="records") # list with var names and values

    item = {}
    for var_val in vars_vals:
        var_name = var_val["name"]
        var_val = var_val["value"]

        if str(var_val["value"]) == "nan": # fixing nan values
            var_val = ""

        item[var_name] = var_val
    
    item["id"] = int.from_bytes(hashlib.sha256(item["path"]).digest()[:4], 'little') # obtain unique id from path
    item["batch_id"] = batch_id # add batch_id to item

    # write to database
    process_item(item, conn)

    logging.info(f"write_file_to_psql: file written successfully")


def write_to_psql(input_path: str, batch_id: int):
    """
    Writes all csv files in a folder to the specified table
    """

    logging.info(f"write_to_psql: writing all files in {input_path} to table_1_reports_metatada")

    conn = create_connection()
    files_paths = glob(input_path + "/*.csv", recursive=True) # reading excels from input path

    for file_path in tqdm(files_paths): # write each file to the database table
        write_file_to_psql(file_path, batch_id, conn)

    logging.info(f"write_to_psql: all files written")


if __name__ == "__main__":
    write_to_psql(csv_tables_dict["input_path"], csv_tables_dict["batch_id"])
