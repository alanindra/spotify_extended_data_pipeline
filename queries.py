import config

query = {
    "stream_table":f""" 
        select
            endTime end_ts
            , DATE(endTime, '{config.utc_timezone}') AS end_date
            , STRFTIME('%H:%M', endTime, '{config.utc_timezone}') AS end_time_hm
            , STRFTIME('%H', endTime, '{config.utc_timezone}') AS end_time_h
            , artistName artist_name
            , trackName track_name
            , msPlayed ms_played
            , msPlayed / 1000 as sec_played
            , msPlayed / 60000 as min_played
        from 
            {config.df_table_name_mapping["stream"]}
        where msPlayed <> 0
    """,

    "extended_stream_table":f""" 
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
            , STRFTIME('%H:%M', datetime(offline_timestamp, 'unixepoch', '{config.utc_timezone}')) AS offline_time_hm
            , STRFTIME('%H', datetime(offline_timestamp, 'unixepoch', '{config.utc_timezone}')) AS offline_time_h
            , incognito_mode
        from 
            {config.df_table_name_mapping["extended_stream"]}
        where ms_played <> 0
    """
}
