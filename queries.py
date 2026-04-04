import json
import logging

CONFIG_PATH = 'path'

try:
    with open(CONFIG_PATH, 'r') as file:
        config = json.load(file)
except FileNotFoundError:
    logging.error("Configuration file not found.")
    raise
except Exception as e:
    logging.error(f"Unexpected error occurred {e}.")
    raise

class Queries:
    """  
    List of queries to transform tables
    """

    def __init__(self, config):
        self.config = config

    def query_extended_stream_table(self, config):
        """ 
        Creating a daily snapshot table of Spotify's extended stream data
        Enrich the table with new cols, corresponding to config 
        """

        query = f""" 
            select 
                DATE(ts, '{config.utc_timezone}') AS date
                , STRFTIME('%H:%M', ts, '{config.utc_timezone}') AS time_hm
                , STRFTIME('%H', ts, '{config.utc_timezone}') AS time_h
                , ms_played / 1000 as sec_played
                , ms_played / 60000 as min_played
                -- , ms_played / 3600000 as hour_played
                , *
            from 
                {config.extended_stream}
                extended_stream_table
            where skipped = 0 -- filter out skipped songs
        """ 

        return query
    
