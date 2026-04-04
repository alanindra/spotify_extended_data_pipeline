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
        Stream = audio, video
        Stream must be longer than 0ms
        Enrich the table with new cols, corresponding to config 
        """

        query = f"""
            select 
                ts timestamp
                , DATE(ts, '{config.utc_timezone}') AS date
                , STRFTIME('%H:%M', ts, '{config.utc_timezone}') AS time_hm
                , STRFTIME('%H', ts, '{config.utc_timezone}') AS time_h
                , ms_played
                , ms_played / 1000 as sec_played
                , ms_played / 60000 as min_played
                , platform
                , conn_country
                , master_metadata_track_name
                , master_metadata_album_artist_name
                , master_metadata_album_album_name
                , spotify_track_uri
                , episode_name
                , episode_show_name
                , spotify_episode_uri
                , audiobook_title
                , audiobook_uri
                , audiobook_chapter_uri
                , audiobook_chapter_title
                , reason_start
                , reason_end
                , shuffle
                , skipped
                , offline
                , datetime(offline_timestamp, 'unixepoch', '{config.utc_timezone}') offline_timestamp
                , STRFTIME('%H:%M', datetime(offline_timestamp, 'unixepoch', '{config.utc_timezone})) AS offline_time_hm
                , STRFTIME('%H', datetime(offline_timestamp, 'unixepoch', '{config.utc_timezone}')) AS offline_time_h
                , incognito_mode
            from 
                {config.extended_stream}
            where ms_played <> 0
        """

        return query
    
