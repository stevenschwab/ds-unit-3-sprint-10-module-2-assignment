'''Data Pipeline for moving titanic csv data to postgres hosted db'''

import psycopg2
from psycopg2.extras import execute_values
import sqlite3
from dotenv import load_dotenv
import os
import pandas as pd
from queries import *

# Load the titanic.csv file
csv_file = 'titanic.csv'

# read the data from the csv
df = pd.read_csv(csv_file)

# look at the shape, head, statistics, more info, and check for missing values
# print(df.shape)
# print(df)
# print(df.describe())
# print(df.info())
# print(df.isnull().sum())

# Load .env file
load_dotenv()

# Get secrets
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_user = os.getenv("DB_USER")
db_name = os.getenv("DB_NAME")

# connecting to PostgreSQL DB
pg_conn = psycopg2.connect(f"postgres://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=require")
pg_curs = pg_conn.cursor()

# define db file and table name
db_file = 'titanic.sqlite3'
table_name = 'titanic'

# connect to sqlite3 db
sl_conn = sqlite3.connect(db_file)
sl_curs = sl_conn.cursor()

# generic execute_query function
def execute_query(curs, query=GET_PASSENGERS):
    result = curs.execute(query)
    return result

# insert statement for postgres
def populate_pg_titanic_table(curs, conn, passenger_list):
    INSERT_STATEMENT = """
        INSERT INTO titanic (Survived, Pclass, Name, Sex, Age, Siblings_Spouses_Aboard, Parents_Children_Aboard, Fare)
        VALUES %s;
    """
    execute_values(curs, INSERT_STATEMENT, passenger_list)
    conn.commit()
    print("Data inserted successfully")

# close connections
def close_connections():
    sl_curs.close()
    sl_conn.close()
    pg_curs.close()
    pg_conn.close()

if __name__ == '__main__':
    # load data from csv to sqlite3
    print('Loading data from csv into sqlite3')
    df.to_sql(table_name, sl_conn, if_exists='replace', index=False)
    print('Data loaded into sqlite3')

    # get passenger data from sqlite3
    SL_PASSENGERS = execute_query(sl_curs, GET_PASSENGERS).fetchall()
    print(SL_PASSENGERS[:5])

    # create titanic table in postgres
    print(f'Creating {table_name} in postgres')
    try:
        execute_query(pg_curs, DROP_TITANIC_TABLE)
        execute_query(pg_curs, CREATE_TITANIC_TABLE)

        # insert data into postgres
        populate_pg_titanic_table(pg_curs, pg_conn, SL_PASSENGERS)
    except Exception as e:
        print(f"Error: {e}")
        pg_conn.rollback()
    
    close_connections()