import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: issue dropping table")
            print(e)
        

# Helper function to validate if all tables were created successfully in Redshift
# Uncomment function if you need validation.
'''
def show_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    query = "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"
    #print("Query :", query)
    try:
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            print(row)
    except psycopg2.Error as e:
        print("Error: issue listing table")
        print(e)
        
'''

def create_tables(cur, conn):
    """
    Creates each table using the query in `create_table_queries` list. 
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: issue creating table")
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

    # Function call that execute queries to drop all tables if already exists
    # This is to ensure you have a clean database tables
    drop_tables(cur, conn)
    
    # Function call that execute queries to create all tables needed for etl
    create_tables(cur, conn)

    try:
        conn.close()
    except Exception as e:
        print("Error: issue in closing DB connection")
        print(e)

if __name__ == "__main__":
    main()