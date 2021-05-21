import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ 
    This function executes all queries to run COPY command to load staging tables
    
    Parameters:
        cur : The DB connection cursor handle to allow executing query
        conn: The DB connection object
    Raises:
        Exception: An error occured while loading data into a table
    
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)


def insert_tables(cur, conn):
    """ 
    This function executes all queries to load data into final tables from 
    staging tables which was earlier populated using COPY command.
    
    Parameters:
        cur : The DB connection cursor handle to allow executing query
        conn: The DB connection object
    
    Raises:
        Exception: An error occured while inserting values into a table
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)


def main():
    # First read configurations value from given configuration file.
    config = configparser.ConfigParser()
    config.read('dwh.cfg')


    # Get a connection object for the Redshift cluster.
    # Configuration value that are needed for establishing connection is provided.
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    except Exception as e:
        print("Error: Issue in getting connection object")
        print(e)
    
    # Function call to load staging tables from the datasets source location
    load_staging_tables(cur, conn)
    
    #Function call to load final tables from the staging tables.
    insert_tables(cur, conn)

    try:
        conn.close()
    except Exception as e:
        print("Error: issue in closing DB connection")
        print(e)


if __name__ == "__main__":
    main()