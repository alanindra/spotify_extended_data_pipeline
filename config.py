from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

utc_timezone = "+7 hours"

df_table_name_mapping = {
    "extended_audio_stream": "extended_audio_stream_table",
    "extended_video_stream": "extended_video_stream_table",
    "extended_stream": "extended_stream_table",
    "stream": "stream_table",
    "tracks": "tracks_table",
    "albums": "albums_table",
    "artist": "artist_table",
}

path = {
    "input": BASE_DIR / "input",
    "metadata": BASE_DIR / "input" / "metadata",
    "stream_history": BASE_DIR / "input" / "stream_history",
    "extended_stream_history": BASE_DIR / "input" / "extended_stream_history",
    "output": BASE_DIR / "output",
    "processed": BASE_DIR / "processed",
    "logs": BASE_DIR / "logs",
}