# Install Dependencies

To install the needed dependencies for the ETL Pipeline,
run the following command in your terminal

``pip install -r requirements.txt``

# Config File

The ETL Script requires that you create a ``config.json`` file containing 
your Youtube Data API key and the ids of the playlists to extract 
comments from. The output directory for the ETL pipeline may also be specified.
If this is not provided, the file will be saved to `your/etl/directory/data`. 
For example:

```json
{
  "API_KEY": "<YOUR_API_KEY>",
  "playlist_ids": [
    "PLfoNZDHitwjU-UZEPlWHRW7SYO20fO6v0", // F1 2021 Race Highlights Playlist ID
    "PLfoNZDHitwjX-oU5YVAkfuXkALZqempRS"
  ],
  "output_directory": "C:\\path\\to\\your\\output\\directory" // optional field
}
```

# Start ETL Pipeline

To start the ETL Pipeline, run the following command in your terminal

``python main.py``

## Note
* Errors may occasionally occur with the Youtube Data API. The ETL Script will 
try a maximum of three (3) times to make the relevant API call before raising
an error and stopping the extraction process.
* To operate within the free quota usage limit of the Youtube Data API, we only 
extract from the F1 2021 Race Highlights Playlist