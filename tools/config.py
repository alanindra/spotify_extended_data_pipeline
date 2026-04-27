from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

utc_timezone = "+7 hours"
df_table_name_mapping = {
    "extended_audio_stream": "extended_audio_stream_table",
    "extended_video_stream": "extended_video_stream_table",
    "extended_stream": "extended_stream_table",
    "stream": "stream_table",
    "tracks": "tracks_table",
    "albums": "albums_table",
    "artists": "artist_table",
}
path = {
    "input": BASE_DIR / "input",
    "metadata": BASE_DIR / "input" / "metadata",
    "stream_history": BASE_DIR / "input" / "stream_history",
    "extended_stream_history": BASE_DIR / "input" / "extended_stream_history",
    "output": BASE_DIR / "output",
    "processed": BASE_DIR / "processed",
    "logs": BASE_DIR / "logs",
    "master_table": BASE_DIR / "output" / "master_table.csv"
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