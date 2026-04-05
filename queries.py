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

ssot = {
    "ssot_table":f""" 
        select 
            extended.* 
            , artist.uri spotify_artist_uri
            , album.uri spotify_album_uri
        from {query["extended_stream_table"]} extended
            left join artists_table artist
                on extended.master_metadata_album_artist_name = artist.name
            left join albums_table album
                on extended.master_metadata_album_artist_name = album.artist
                and extended.master_metadata_album_album_name = album.album
    """
}

metrics = {
    "skip_rate":f""" 
        with
            track_skipped_status as (
                select 
                    master_metadata_track_name
                    , master_metadata_album_artist_name
                    , master_metadata_album_album_name
                    , skipped
                    , count(*) count
                from 
                    {ssot["ssot_table"]}
                where 
                    master_metadata_album_artist_name is not null
                group by 
                    master_metadata_track_name
                    , master_metadata_album_artist_name
                    , master_metadata_album_album_name
                    , skipped
            )

        select 
            master_metadata_track_name
            , master_metadata_album_artist_name
            , master_metadata_album_album_name
            , sum(case when skipped = 1 then count else 0 end) as count_skipped
            , sum(case when skipped = 0 then count else 0 end) as count_unskipped
            , sum(count) count_total
            , (
                cast(sum(case when skipped = 1 then count else 0 end) as real)
                / (sum(count))
            ) as skip_rate
        from 
            track_skipped_status 
        group by 
            master_metadata_track_name
            , master_metadata_album_album_name
            , master_metadata_album_artist_name
        order by count_total desc
        ;
    """
}

