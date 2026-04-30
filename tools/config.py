from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

utc_timezone = "+7 hours"
df_table_name_mapping = {
    "extended_audio_stream": "extended_audio_stream_table",
    "extended_video_stream": "extended_video_stream_table",
    "extended_stream": "extended_stream_table",
    "stream": "stream_table",
    "extended_stream": "extended_stream_table",
    "tracks": "tracks_table",
    "albums": "albums_table",
    "artists": "artist_table",
    "transformation_history": "transformation_history"
}
path = {
    "input": BASE_DIR / "input",
    "metadata": BASE_DIR / "input" / "metadata",
    "stream_history": BASE_DIR / "input" / "stream_history",
    "extended_stream_history": BASE_DIR / "input" / "extended_stream_history",
    "output": BASE_DIR / "output",
    "processed": BASE_DIR / "processed",
    "logs": BASE_DIR / "logs",
    "raw_tables": BASE_DIR / "output" / "raw_tables",
    "master_tables": BASE_DIR / "output" / "master_tables",
    "table_history": BASE_DIR / "output" / "table_history",

    "raw_extended_stream_table": BASE_DIR / "output" / "raw_tables" / (df_table_name_mapping['extended_stream'] + ".csv"),
    "raw_tracks_table": BASE_DIR / "output" / "raw_tables" / (df_table_name_mapping['tracks'] + ".csv"),
    "raw_albums_table": BASE_DIR / "output" / "raw_tables" / (df_table_name_mapping['albums'] + ".csv"),
    "raw_artists_table": BASE_DIR / "output" / "raw_tables" / (df_table_name_mapping['artists'] + ".csv"),

    "master_extended_stream_table": BASE_DIR / "output" / "master_tables" / (df_table_name_mapping['extended_stream'] + ".csv"),
    "master_tracks_table": BASE_DIR / "output" / "master_tables" / (df_table_name_mapping['tracks'] + ".csv"),
    "master_albums_table": BASE_DIR / "output" / "master_tables" / (df_table_name_mapping['albums'] + ".csv"),
    "master_artists_table": BASE_DIR / "output" / "master_tables" / (df_table_name_mapping['artists'] + ".csv"),

    "transformation_history_table": BASE_DIR / "output" / "transformation_history" / (df_table_name_mapping['transformation_history'] + ".csv")
}
file_names = {
    "stream": "StreamingHistory_music*",
    "extended_stream": "Streaming_History*",
    "metadata": "YourLibrary.json"
}
metadata_obejct = {
    "tracks": "tracks",
    "albums": "albums",
    "artists": "artists"
}
features = {
    "table_enrichment":"false"
}