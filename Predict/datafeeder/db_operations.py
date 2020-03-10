import psycopg2
from configparser import ConfigParser
#from psycopg2 import connect
import time
#from builtins import set

#constants
config_file_path = 'datafeeder/conn.config'
section_name = 'postgresql_conn_data'

class db_operations:
    def __init__(self):
        
        return
    def connect(self):
        self.get_connection_by_config()
        return self._conn.status == psycopg2.extensions.STATUS_READY

    def disconnect(self):
        self._conn.close()
 
    def get_connection_by_config(self):
        if(len(config_file_path) > 0 and len(section_name) > 0):
            config_parser = ConfigParser()
            config_parser.read(config_file_path)
            if(config_parser.has_section(section_name)):
                config_params = config_parser.items(section_name)
                db_conn_dict = {}
                for config_param in config_params:
                    key = config_param[0]
                    value = config_param[1]
                    db_conn_dict[key] = value
                conn = psycopg2.connect(**db_conn_dict)
        self._conn = conn
        
    def found_duplicate(self, rss_url, title):
        cursor = self._conn.cursor()
        postgres_select_query = """ SELECT count(*)
            FROM public.daily_logs
            where rss_source = %s
            and title = %s;"""
        record_to_search = (rss_url, title)
        cursor.execute(postgres_select_query, record_to_search)
        res = cursor.fetchone()
        cursor.close()
        return res[0] > 0
        
    
    def insert(self, rss_url, title, link ):
        cursor = self._conn.cursor()
        postgres_insert_query = """ INSERT INTO public.daily_logs
        (rss_source, time_stamp, title, url, status, extra_note)
        VALUES (%s, %s, %s, %s, %s, %s);"""
        record_to_insert = (rss_url, time.asctime(), title, link, 'new', '')
        cursor.execute(postgres_insert_query, record_to_insert)
        self._conn.commit()
        cursor.close()   
        return 
        
    def get_content_urls(self):
        cursor = self._conn.cursor()
        postgres_select_query = """ SELECT log_id, url
            FROM public.daily_logs
            where status = 'new';"""
        cursor.execute(postgres_select_query)
        res = cursor.fetchall()
        cursor.close();
        return res
    def insert_content(self, log_id, content):
            #insert the real data from url.
        cursor = self._conn.cursor()
        postgres_insert_query = """ INSERT INTO public.content(log_id, content)
            VALUES(%s, %s);
            """
        record_to_insert = (log_id, content)
        cursor.execute(postgres_insert_query, record_to_insert)
        self._conn.commit()
        #upate the daily log table to set the flag
        cursor = self._conn.cursor()
        postgres_update_query = "UPDATE public.daily_logs SET status= 'retrieved'  WHERE log_id = {};"
        postgres_update_query = postgres_update_query.format(log_id)
        print(postgres_update_query)
        cursor.execute(postgres_update_query)
        self._conn.commit() 
        
        cursor.close()   
        return 
        