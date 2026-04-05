import pandas as pd
import logging
import json
from config import path, file_names, metadata_obejct

logger = logging.getLogger(__name__)

class SsotTable:
    def __init__(self, df=None):
        self.path = path
        self.file_names = file_names
        self.metadata_obejct = metadata_obejct
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
    
    def read_metadata_file(self):
        metadata_dir = self.path["metadata"]
        metadata_file = metadata_dir / self.file_names["metadata"]

        if not metadata_dir.exists():
            logger.error("Directory not found: %s", metadata_dir)
            raise FileNotFoundError(f"Directory not found: {metadata_dir}")

        if not metadata_dir.is_dir():
            logger.error("Path is not a directory: %s", metadata_dir)
            raise NotADirectoryError(f"Path is not a directory: {metadata_dir}")

        if not metadata_file.exists():
            logger.error("Metadata file not found: %s", metadata_file)
            raise FileNotFoundError(f"Metadata file not found: {metadata_file}")
        
        try:
            with open(metadata_file, "r", encoding="utf-8") as f:
                metadata = json.load(f)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse JSON file: %s", metadata_file)
            raise ValueError(f"Failed to parse JSON file: {metadata_file}") from e

        return metadata
    
    def create_tracks_table(self):
        df = self.read_metadata_file()
        return pd.json_normalize(df[self.metadata_obejct["tracks"]])

    def create_albums_table(self):
        df = self.read_metadata_file()
        return pd.json_normalize(df[self.metadata_obejct["albums"]])
    
    def create_artists_table(self):
        df = self.read_metadata_file()
        return pd.json_normalize(df[self.metadata_obejct["artists"]])

    def create_extended_stream_table(self):
        extended_stream_history_dir = self.path["extended_stream_history"]

        if not extended_stream_history_dir.exists():
            logger.error("Directory not found: %s", extended_stream_history_dir)
            raise FileNotFoundError(f"Directory not found: {extended_stream_history_dir}")

        if not extended_stream_history_dir.is_dir():
            logger.error("Path is not a directory: %s", extended_stream_history_dir)
            raise NotADirectoryError(f"Path is not a directory: {extended_stream_history_dir}")
        
        files = list(extended_stream_history_dir.glob(self.file_names["extended_stream"]))

        if not files:
            logger.error(
                "No matching files found in %s for '%s'",
                extended_stream_history_dir,
                self.file_names["extended_stream"]
            )
            raise FileNotFoundError(
                f"No matching files found in {extended_stream_history_dir} "
                f"for '{self.file_names["extended_stream"]}"
            )

        extended_stream_table = []

        for file in files:
            try:
                extended_stream_table.append(pd.read_json(file))
            except ValueError as e:
                logger.error("Failed to read JSON file: %s", file)
                raise ValueError(f"Failed to read JSON file: {file}") from e

        return pd.concat(extended_stream_table, ignore_index=True)

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

    # def enrich_audio_features(self, df):
