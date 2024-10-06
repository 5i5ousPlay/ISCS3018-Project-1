from functions import *
from datetime import datetime


class ETL:
    playlist_ids = None

    def __init__(self):
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)

            if not config or 'playlist_ids' not in config:
                raise KeyError("Missing required key 'playlist_ids' in config.json")

            self.playlist_ids = config['playlist_ids']

        except json.JSONDecodeError:
            raise ValueError("The config.json file contains invalid JSON. Please check the file format.")

    def _extract(self) -> pd.DataFrame:
        try:
            print("Starting extraction. Please ensure network connectivity. This may take a while...")
            dataframes = []
            for pid in self.playlist_ids:
                playlist_comments = extract_playlist_comments(playlistId=pid)
                dataframes.append(playlist_comments)

            return pd.concat(dataframes, axis=0).reset_index(drop=True)
        except Exception as e:
            raise Exception(str(e))

    def _transform(self, data: pd.DataFrame) -> pd.DataFrame:
        print("Transforming data. Please wait. This may take a while...")
        clean_date(data)
        clean_text(data)
        clean_text_sentiment_analysis(data)
        return data

    def _load(self, data: pd.DataFrame):
        print("Loading data...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"extracted_data_{timestamp}.csv"

        data.to_csv(filename, index=False)
        print(f"Data saved to {filename}")

    def start(self):
        data = self._extract()
        processed = self._transform(data)
        self._load(processed)
