# [WIP] Spotify Data Pipeline

Create master table for your Spotify extended stream data and update it every time you retrieved your latest Spotify extended stream data, so you can explore your streaming history without having to wait for the end of year :)

## Getting started

- input
    - metadata
        - Contains YourLibrary.json file
    - streaming_history
        - Contains StreamingHistory_music*.json files
    - extended_streaming_history
        - Contains Streaming_History_Audio*.json files
        - Contains Streaming_History_Video*.json files

Once processed, the data will be moved to processed dir

- output
    - metadata
        - metadata_{date}_{counter} (info latest updated date per table)
    - tables
        - spotify_master_table.csv
        - spotify_artist_table.csv
        - spotify_trackts_table.csv
        - spotify_albums_table.csv
        - spotify_churn_monthly_snapshot.csv

- processed
    - {time}
        - Contains untransformed and unenriched tables

- logs
    - must contain table name, update time (upstream for metadata)

- dashboard
    - dashboard.ipynb
        - The dashboard. May make some for different themes
