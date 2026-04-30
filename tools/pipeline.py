import pandas as pd
import logging
import json
from tools import queries, config, queries, logging_setup
from pathlib import Path
from datetime import date
import shutil
from pathlib import Path
from datetime import datetime

logging_setup.setup_logging()
logger = logging.getLogger(__name__)

class Pipeline:
    def __init__(self):
        self.path = config.path
        self.table_name = config.df_table_name_mapping
        self.file_names = config.file_names
        self.query = queries.query
        self._processed_folder = None
        self.event_logger = logging_setup.EventLogger()

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
    
    def _get_processed_folder(self):
        if self._processed_folder is None:
            self._processed_folder = self._create_processed_folder()
        return self._processed_folder

    def _save_to_raw_tables_folder(self, df, name):
        path_output = self.path['raw_tables']
        path = path_output / f"{name}.csv"
        df.to_csv(path, index=False)

    def move_to_processed_dir(self):
        input_path = self.path['input']
        processed_path = self._get_processed_folder()

        fixed_dirs = {
            "extended_stream_history",
            "metadata",
            "stream_history"
        }
        
        for folder in input_path.iterdir():
            if not folder.is_dir():
                continue
            destination = processed_path / folder.name
            counter = 2
            while destination.exists():
                destination = processed_path / f"{folder.name}_{counter}"
                counter += 1
            
            shutil.move(str(folder), str(destination))

            if folder.name in fixed_dirs:
                (input_path / folder.name).mkdir(parents=True, exist_ok=True)

    def create_stream_table(self):
        stream_history_dir = self.path["stream_history"]

        if not stream_history_dir.exists():
            logger.warning("No matching dir found for %s. Creating a new dir", stream_history_dir)
            stream_history_dir.mkdir(parents=True, exist_ok=True)

        if not stream_history_dir.is_dir():
            logger.error("Path is not a directory: %s", stream_history_dir)
            raise NotADirectoryError(f"Path is not a directory: {stream_history_dir}")
        
        files = list(stream_history_dir.glob(self.file_names["stream"]))

        if not files:
            logger.warning(
                "No matching files found in %s for '%s'",
                stream_history_dir,
                self.file_names["stream"]
            ) 
            self.event_logger.create_event_log(
                event="pipeline_table_transformation",
                table_name=self.table_name['stream'],
                status="table not found"
            )
            return

        stream_table = []

        for file in files:
            try:
                stream_table.append(pd.read_json(file))
            except ValueError as e:
                logger.error("Failed to read JSON file: %s", file)
                raise ValueError(f"Failed to read JSON file: {file}") from e

        stream_table = pd.concat(stream_table, ignore_index=True)
        stream_table = self.psql(self.query["stream_table"], {"stream_table":stream_table})
        
        self._save_to_raw_tables_folder(stream_table)

        n_row, n_col = stream_table.shape

        self.event_logger.create_event_log(
            event="pipeline_table_transformation",
            table_name=self.table_name['stream'],
            status="success",
            state="create new table",
            message={"n_row":n_row, "n_col":n_col}
        )

        return stream_table
    
    def _read_metadata_file(self):
        metadata_dir = self.path["metadata"]
        metadata_file = metadata_dir / self.file_names["metadata"]

        if not metadata_dir.exists():
            logger.warning("No matching dir found for %s. Creating a new dir", metadata_dir)
            metadata_dir.mkdir(parents=True, exist_ok=True)

        if not metadata_dir.is_dir():
            logger.error("Path is not a directory: %s", metadata_dir)
            raise NotADirectoryError(f"Path is not a directory: {metadata_dir}")

        if not metadata_file.exists():
            logger.warning(
                "No matching files found in %s for '%s'",
                metadata_dir,
                metadata_file
            )
            return
        
        try:
            with open(metadata_file, "r", encoding="utf-8") as f:
                metadata = json.load(f)
        except json.JSONDecodeError as e:
            logger.warning("Failed to parse JSON file: %s", metadata_file)
            raise ValueError(f"Failed to parse JSON file: {metadata_file}") from e

        return metadata
    
    def create_tracks_table(self):
        df = self._read_metadata_file()
        if df is None:
            logger.warning("Source table %s does not exist. Skipping process.", self.table_name['tracks'])   
            self.event_logger.create_event_log(
                event="pipeline_table_transformation",
                table_name=self.table_name['tracks'],
                status="failed",
                state="nonexistent source table",
                message=f"No file inside {self.path['metadata']} found"
            )
            return
        
        tracks_table = pd.json_normalize(df["tracks"])
        raw_tracks_table_path = self.path['raw_tracks_table']

        if raw_tracks_table_path.exists():
            raw_tracks_table = pd.read_csv(self.path['raw_tracks_table'])
            updated_tracks_table = pd.concat(
                [tracks_table, raw_tracks_table], 
                ignore_index=True
            ).drop_duplicates()
            
            n_row, n_col = updated_tracks_table.shape

            logger.info("Table: %s successfully updated.", self.table_name['tracks'])   
            self.event_logger.create_event_log(
                event="pipeline_table_transformation",
                table_name=self.table_name['tracks'],
                status="success",
                state="update existing table",
                message={"n_row":n_row, "n_col":n_col}
            )

            self._save_to_raw_tables_folder(updated_tracks_table, self.table_name['tracks'])
        
        else:
            n_row, n_col = tracks_table.shape

            logger.info("Table: %s successfully created.", self.table_name['tracks'])
            self.event_logger.create_event_log(
                event="pipeline_table_transformation",
                table_name=self.table_name['tracks'],
                status="success",
                state="create new table",
                message={"n_row":n_row, "n_col":n_col}
            )

            self._save_to_raw_tables_folder(tracks_table, self.table_name['tracks'])

    def create_albums_table(self):
        df = self._read_metadata_file()
        if df is None:
            logger.warning("Source table %s does not exist. Skipping process.", self.table_name['albums'])   
            self.event_logger.create_event_log(
                event="pipeline_table_transformation",
                table_name=self.table_name['albums'],
                status="failed",
                state="nonexistent source table",
                message={f"No file found inside {self.path["metadata"]}"}
            )
            return
       
        albums_table = pd.json_normalize(df["albums"])
        raw_albums_table_path = self.path['raw_albums_table']
        
        if raw_albums_table_path.exists():
            raw_albums_table = pd.read_csv(self.path['raw_albums_table'])
            updated_albums_table = pd.concat(
                [albums_table, raw_albums_table], 
                ignore_index=True
            ).drop_duplicates()

            n_row, n_col = updated_albums_table.shape

            logger.info("Table: %s successfully updated.", self.table_name['albums'])   
            self.event_logger.create_event_log(
                event="pipeline_table_transformation",
                table_name=self.table_name['albums'],
                status="success",
                state="update existing table",
                message={"n_row":n_row, "n_col":n_col}
            )

            self._save_to_raw_tables_folder(updated_albums_table, self.table_name['albums'])

        else:
            n_row, n_col = albums_table.shape

            logger.info("Table: %s successfully created.", self.table_name['albums'])   
            self.event_logger.create_event_log(
                event="pipeline_table_transformation",
                table_name=self.table_name['albums'],
                status="success",
                state="create new table",
                message={"n_row":n_row, "n_col":n_col}
            )

            self._save_to_raw_tables_folder(albums_table, self.table_name['albums'])
    
    def create_artists_table(self):
        df = self._read_metadata_file()
        if df is None:
            logger.warning("Source table %s does not exist. Skipping process.", self.table_name['artists'])   
            self.event_logger.create_event_log(
                event="pipeline_table_transformation",
                table_name=self.table_name['artists'],
                status="failed",
                state="nonexistent source table",
                message=f"No file inside {self.path['metadata']} found"
            )
            return

        artists_table = pd.json_normalize(df["artists"])
        raw_artist_table_path = self.path['raw_artists_table']
        
        if raw_artist_table_path.exists():
            raw_artists_table = pd.read_csv(self.path['raw_artists_table'])
            updated_artists_table = pd.concat(
                [artists_table, raw_artists_table], 
                ignore_index=True
            ).drop_duplicates()

            n_row, n_col = updated_artists_table.shape

            logger.info("Table: %s successfully updated.", self.table_name['artists'])   
            self.event_logger.create_event_log(
                event="pipeline_table_transformation",
                table_name=self.table_name['artists'],
                status="success",
                state="update existing table",
                message={"n_row":n_row, "n_col":n_col}
            )

            self._save_to_raw_tables_folder(updated_artists_table, self.table_name['artists'])
        
        else:
            n_row, n_col = artists_table.shape

            logger.info("Table: %s successfully created.", self.table_name['artists'])   
            self.event_logger.create_event_log(
                event="pipeline_table_transformation",
                table_name=self.table_name['artists'],
                status="success",
                state="create new table",
                message={"n_row":n_row, "n_col":n_col}
            )

            self._save_to_raw_tables_folder(artists_table, self.table_name['artists'])        

    def create_extended_stream_table(self):
        extended_stream_history_dir = self.path["extended_stream_history"]

        if not extended_stream_history_dir.exists():
            logger.warning("No matching dir found for %s. Creating a new dir", extended_stream_history_dir)
            extended_stream_history_dir.mkdr(parents=True, exist_ok=True)

        if not extended_stream_history_dir.is_dir():
            logger.error("Path is not a directory: %s", extended_stream_history_dir)
            raise NotADirectoryError(f"Path is not a directory: {extended_stream_history_dir}")
        
        files = list(extended_stream_history_dir.glob(self.file_names["extended_stream"]))

        if not files:
            logger.warning("Source table %s does not exist. Skipping process.", self.table_name['extended_stream'])
            self.event_logger.create_event_log(
                event="pipeline_table_transformation",
                table_name=self.table_name['extended_stream'],
                status="failed",
                state="nonexistent source table",
                message={f"No file found inside {extended_stream_history_dir}"}
            )
            return

        extended_stream_table = []

        for file in files:
            try:
                extended_stream_table.append(pd.read_json(file))
            except ValueError as e:
                logger.error("Failed to read JSON file: %s", file)
                raise ValueError(f"Failed to read JSON file: {file}") from e
    
        extended_stream_table = pd.concat(extended_stream_table, ignore_index=True)
        raw_extended_stream_table_path = self.path["raw_extended_stream_table"]
        
        if raw_extended_stream_table_path.exists():
            raw_extended_stream_table = pd.read_csv(self.path["raw_extended_stream_table"])
            updated_extended_stream_table = pd.concat(
                [extended_stream_table, raw_extended_stream_table], 
                ignore_index=True
            ).drop_duplicates()

            n_col, n_row = updated_extended_stream_table.shape

            logger.info("Table: %s successfully updated.", self.table_name['extended_stream'])   
            self.event_logger.create_event_log(
                event="pipeline_table_transformation",
                table_name=self.table_name['extended_stream'],
                status="success",
                state="update existing table",
                message={"n_row":n_row, "n_col":n_col}
            )

            self._save_to_raw_tables_folder(updated_extended_stream_table, self.table_name['extended_stream'])

        else:
            n_col, n_row = extended_stream_table.shape

            logger.info("Table: %s successfully created.", self.table_name['extended_stream'])   
            self.event_logger.create_event_log(
                event="pipeline_table_transformation",
                table_name=self.table_name['extended_stream'],
                status="success",
                state="create new table",
                message={"n_row":n_row, "n_col":n_col}
            )

            self._save_to_raw_tables_folder(extended_stream_table, self.table_name['extended_stream'])

    def create_table_history_logs(self):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        folder = self.path['table_history']
        folder.mkdir(parents=True, exist_ok=True)
        
        file_path = folder / f"table_history_{timestamp}.json"
        self.event_logger.save_event_log(file_path)

    # def create_table_transformation_history_table(self):

    # TODO #1
    # def enrich_table(self, df):
        """
        extended_stream_table + music attr data
        """
        # extended_stream_table = self.create_extended_stream_table()

    # def _execute_sql_query(self, sql_query):

    # def _read_yaml_resource(self, yaml_resource):

    #     return cols_format, table_destination_name, table_source_name

    # def run_data_jobs():
    #     for job in jobs:
    #         table = _execute_sql_query()
    #         cols_format, table_destination_name, table_source_name = _read_yaml_resource()
    
    # def extract_table_lineage():