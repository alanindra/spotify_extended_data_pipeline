import pandas as pd
import logging
import json
from tools import queries, config, queries
from pandasql import sqldf
from pathlib import Path
from datetime import date
import shutil

logger = logging.getLogger(__name__)

class Pipeline:
    def __init__(self):
        self.path = config.path
        self.table_name = config.df_table_name_mapping
        self.file_names = config.file_names
        self.query = queries.query
        self._output_folder = None
        self._processed_folder = None

    def psql(self, query, env):
        return sqldf(query, env)
    
    def _create_output_folder(self):
        output_path = Path(self.path['output'])
        date_today = date.today().isoformat()

        path = output_path / date_today
        if not path.exists():
            path.mkdir(parents=True, exist_ok=False)
            return path 
        
        counter = 2  
        while True:
            path_counter = output_path / f"{date_today}_{counter}"
            if not path_counter.exists():
                path_counter.mkdir(parents=True, exist_ok=False)
                return path_counter
            counter += 1

    def _create_processed_folder(self):
        processed_path = Path(self.path['processed'])
        date_today = date.today().isoformat()

        path = processed_path / date_today
        if not path.exists():
            path.mkdir(parents=True, exist_ok=False)
            return path 
        
        counter = 2  
        while True:
            path_counter = processed_path / f"{date_today}_{counter}"
            if not path_counter.exists():
                path_counter.mkdir(parents=True, exist_ok=False)
                return path_counter
            counter += 1
    
    def _get_output_folder(self):
        if self._output_folder is None:
            self._output_folder = self._create_output_folder()
        return self._output_folder
    
    def _get_processed_folder(self):
        if self._processed_folder is None:
            self._processed_folder = self._create_processed_folder()
        return self._processed_folder

    def _save_to_output(self, df, name):
        path = self._get_output_folder / f"{name}.csv"

        if not path.exists():
            df.to_csv(path, index=False)
            return path

        counter = 2
        while True:
            path_counter = self._output_folder / f"{name}_{counter}.csv"
            if not path_counter.exists():
                df.to_csv(path_counter, index=False)
                return path_counter
            counter += 1

    def move_to_processed_dir(self):
        input_path = self.path['input']
        processed_path = self._get_processed_folder()
        
        for folder in input_path.iterdir():
            if not folder.is_dir():
                continue
            destination = processed_path / folder.name
            counter = 2
            while destination.exists():
                destination = processed_path / f"{folder.name}_{counter}"
                counter += 1
            
            shutil.move(str(folder), str(destination))

    def create_stream_table(self):
        stream_history_dir = self.path["stream_history"]

        if not stream_history_dir.exists():
            logger.error("Directory not found: %s", stream_history_dir)
            raise FileNotFoundError(f"Directory not found: {stream_history_dir}")

        if not stream_history_dir.is_dir():
            logger.error("Path is not a directory: %s", stream_history_dir)
            raise NotADirectoryError(f"Path is not a directory: {stream_history_dir}")
        
        files = list(stream_history_dir.glob(self.file_names["stream"]))

        if not files:
            logger.error(
                "No matching files found in %s for '%s'",
                stream_history_dir,
                self.file_names["stream"]
            )
            raise FileNotFoundError(
                f"No matching files found in {stream_history_dir} "
                f"for '{self.file_names["stream"]}"
            )

        stream_table = []

        for file in files:
            try:
                stream_table.append(pd.read_json(file))
            except ValueError as e:
                logger.error("Failed to read JSON file: %s", file)
                raise ValueError(f"Failed to read JSON file: {file}") from e

        stream_table = pd.concat(stream_table, ignore_index=True)
        stream_table = self.psql(self.query["stream_table"], {"stream_table":stream_table})
        
        self._save_to_output(stream_table)

        return stream_table
    
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
        df = pd.json_normalize(df["tracks"])
        self._save_to_output(df, self.table_name['tracks'])
        return df

    def create_albums_table(self):
        df = self.read_metadata_file()
        df = pd.json_normalize(df["albums"])
        self._save_to_output(df, self.table_name['albums'])
        return df
    
    def create_artists_table(self):
        df = self.read_metadata_file()
        df = pd.json_normalize(df["artists"])
        self._save_to_output(df, self.table_name['artists'])        
        return df

    def create_extended_stream_table(self, is_master_table=False):
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

        extended_stream_table = pd.concat(extended_stream_table, ignore_index=True)
        extended_stream_table = self.psql(self.query["extended_stream_table"], {"extended_stream_table":extended_stream_table})
        
        if is_master_table is True:
            extended_stream_table.to_csv(self.path['master_table'])
        else: 
            self._save_to_output(extended_stream_table, self.table_name['extended_stream'])
        
        return extended_stream_table

    # TODO #1
    # def enrich_table(self, df):
        """
        extended_stream_table + music attr data
        """
        # extended_stream_table = self.create_extended_stream_table()

    def update_master_table(self):
        try:
            master_table = pd.read_csv(self.path["master_table"]) # old table
        except FileNotFoundError:
            logger.error("Failed to update table: master_table.csv not found")
            return None
        except Exception as e:
            logger.error(f"Unexpected error {e}")
            return None
        
        new_table = self.create_extended_stream_table() # new table

        updated_table = pd.concat([master_table, new_table],ignore_index=True).drop_duplicates()
        updated_table.to_csv(self.path["master_table"])
        return updated_table