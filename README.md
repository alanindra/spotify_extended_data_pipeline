# Spotify Extended Data Pipeline

Create master data for your Spotify extended stream history and update it every time you retrieved your latest Spotify extended stream history.

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
    - master_table.csv
        - The master data
    - data_{time}
        - Contains transformed and enriched tables by times of processing

- processed
    - {time}
        - Contains untransformed and unenriched tables   

- dashboard
    - dashboard.ipynb
        - The dashboard. May make some for different themes