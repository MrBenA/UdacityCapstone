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


def record_count(cur, conn):
    """
    Data quality check - Confirm table record counts after data load
    - Gets user created tables from database
    - Check for at least 1 record and return table record count
    """
    # get all created tables from db
    cur.execute("SELECT * FROM information_schema.tables WHERE table_schema='public'")
    result = cur.fetchall()

    # create list of tables
    table_list = [table[2] for table in result]

    print('Verifying table record counts...')

    # get row count for each table
    for table_name in table_list:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cur.fetchall()
        if row_count == 0:
            print(f"WARNING, record count for {table_name} is zero '0'")
        else:
            print(f"SUCCESS, {row_count[0][0]} record count for {table_name} looks good!")


def duplicate_record_check(cur):
    """
    Data quality check - Confirm distinct table records after data load
    - Gets user created tables from database
    - Compares table record count against distinct record count
    """
    # get all created tables from db
    cur.execute("SELECT * FROM information_schema.tables WHERE table_schema='public'")
    result = cur.fetchall()

    # create list of tables
    table_list = [table[2] for table in result]

    print('Checking tables for duplicate records...')

    # check each table for duplicates
    for table_name in table_list:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cur.fetchall()
        cur.execute(f"SELECT DISTINCT COUNT(*) FROM {table_name}")
        distinct_count = cur.fetchall()
        if row_count[0][0] == distinct_count[0][0]:
            print(f"GREAT, no duplicate records found in {table_name}!")
        elif distinct_count[0][0] < row_count[0][0]:
            print(f"WARNING, duplicate records found! {distinct_count[0][0]}"
                  f"distinct record count is less than total record count of {row_count[0][0]}")


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
    record_count(cur, conn)
    duplicate_record_check(cur)

    conn.close()


if __name__ == "__main__":
    main()
