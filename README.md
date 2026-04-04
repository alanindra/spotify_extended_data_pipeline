# alans_spotify

## Input
- metadata
    - Contains YourLibrary.json file
- streaming_history
    - Contains StreamingHistory_music*.json files
- extended_streaming_history
    - Contains Streaming_History_Audio*.json files
    - Contains Streaming_History_Video*.json files

Once processed, the data will be moved to processed dir

## Output
- output
    - {time}
        - Contains transformed and enriched tables
- processed
    - {time}
        - Contains untransformed and unenriched tables   
