import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    ''' load Sparkify data from S3 into staging_events and staging_songs tables''' 
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    ''' normalize data from staging_events and staging_songs and insert into songplay, users, songs, artists, and time tables'''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit() #15


def main():
    ''' retreive credentials from dwh.cfg file and execute load_staging_tables and insert_tables'''
    config = configparser.ConfigParser()
    config.read('dwh.cfg') #20

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
