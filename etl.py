import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
#    """
#
#   The purpose of this function is to copy event and song json files from S3 bucket to staging tables in redshift.
#  
#    Args:
#    cur: Database cursor object.
#    con: Connection String to connect to redshift.
#
#    Returns:
#    None
#
#    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print(e)
           
        conn.commit()


def insert_tables(cur, conn):
#    """
#
#   The purpose of this function is insert into fact table(songplay) and dimension(songs,users,time,artists) tables from staging tables.
#  
#    Args:
#    cur: Database cursor object.
#    con: Connection String to connect to redshift.
#
#    Returns:
#    None
#
#    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print(e)
        conn.commit()


def main():
    #Parse the config file 
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #Function to load to staging tables from S3 bucket
    load_staging_tables(cur, conn)
    
    #Function to insert to fact and dimension tables from staging tables
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()