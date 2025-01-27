import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    csv_name = 'output.csv.gz'

    # Download the file using curl
    os.system(f"curl -L {url} -o {csv_name}")
    
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df = pd.read_csv(csv_name, compression='gzip', nrows=1)
    print(pd.io.sql.get_schema(df, name=table_name, con=engine))
    
    
    df_iter = pd.read_csv(csv_name, compression='gzip', iterator=True, chunksize=10000)
    df = next(df_iter)
    
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    
    df.to_sql(name=table_name, con=engine, if_exists='append')
    
    # Insert remaining data in chunks
    while True:
            t_start = time()
            df = next(df_iter)
            
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            
            df.to_sql(name=table_name, con=engine, if_exists='append')
            
            t_end = time()
            print(f'Inserted another chunk... took {t_end - t_start:.3f} seconds')
            
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV Data to Postgres')
    
    parser.add_argument('--user', help='username for PostgreSQL')
    parser.add_argument('--password', help='password for PostgreSQL')
    parser.add_argument('--host', help='host for PostgreSQL')
    parser.add_argument('--port', help='port for PostgreSQL')
    parser.add_argument('--db', help='database name for PostgreSQL')
    parser.add_argument('--table-name', help='name of the table to write the results to')
    parser.add_argument('--url', help='URL of the CSV file')
    
    args = parser.parse_args()
    
    main(args)
