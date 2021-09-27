import configparser
import psycopg2
from sql_queries import copy_table_queries


def load_parquet_tables(cur, conn):
    """
    Executes SQL COPY queries to load partitioned parquet data from S3 datalake to data warehouse tables.
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print('Success, table data loaded!')


def main():
    """
    - Establishes connection with Redshift cluster and LndBikeHire database and gets cursor to it.

    - Loads all table data from partitioned S3 hosted parquet files.

    - Load data from staging tables to fact and dimension tables.

    - Closes the connection.
    """

    config = configparser.ConfigParser()
    config.read('aws/dl.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_parquet_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()