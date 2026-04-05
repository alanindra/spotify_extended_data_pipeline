import pandas as pd
import logging
from config import path

logger = logging.getLogger(__name__)

class SsotTable:
    def __init__(self, df=None):
        self.path = path
        self.df = df
    
    def create_stream_table(self):
        stream_history_dir = self.path["stream_history"]

        if not stream_history_dir.exists():
            logger.error("Directory not found: %s", stream_history_dir)
            raise FileNotFoundError(f"Directory not found: {stream_history_dir}")

        if not stream_history_dir.is_dir():
            logger.error("Path is not a directory: %s", stream_history_dir)
            raise NotADirectoryError(f"Path is not a directory: {stream_history_dir}")
        
        files = list(stream_history_dir.glob("StreamingHistory_music*"))

        if not files:
            logger.error(
                "No matching files found in %s for 'StreamingHistory_music*'",
                stream_history_dir
            )
            raise FileNotFoundError(
                f"No matching files found in {stream_history_dir} "
                f"for 'StreamingHistory_music*'"
            )

        stream_table = []

        for file in files:
            try:
                stream_table.append(pd.read_json(file))
            except ValueError as e:
                logger.error("Failed to read JSON file: %s", file)
                raise ValueError(f"Failed to read JSON file: {file}") from e

        return pd.concat(stream_table, ignore_index=True)

    # def create_album_table(self, df):

    # def create_artist_table(self, df):

    # def create_streaming_history_table(self, df):

    # def create_extended_streaming_history_table(self, df):

    # def enrich_audio_features(self, df):

    # def create_ssot_table(self, df):
        """
        Create one table containing all information of the Spotify data
        Table is a daily snapshot of music stream
        From 2018 to Mar 2026
        Streamed audio before 2025 could have null values due to data incompleteness from non-extended stream data
        """
        # tracks_table = create_tracks_table(self.df)
        # albums_table = create_tracks_table(self.df)
        # artists_table = create_tracks_table(self.df)
        # extended_stream_table = create_extended_streaming_history_table(self.df)

        # join operations
        
        # enriched_table = enrich_audio_features(self, df)

        # return enriched_table
