from tools import pipeline
from tools import config
import pandas as pd

path_input = config.path['input']
pipeline = pipeline.Pipeline()

def retrieve_raw_data():
    # perform data jobs if no new data is inputted
    if not path_input.exists() or not any(path_input.iterdir()):
        return
    pipeline.create_extended_stream_table()
    pipeline.create_albums_table()
    pipeline.create_artists_table()
    pipeline.create_tracks_table()
    # pipeline.create_new_table()

if __name__ == "__main__":
    retrieve_raw_data()
    # pipeline.enrich_extended_stream_table()
    # pipeline.run_data_jobs()
    # pipeline.extract_table_lineage()
    # pipeline.create_table_transformation_history_table()
    pipeline.create_table_history_logs()
    pipeline.move_to_processed_dir()