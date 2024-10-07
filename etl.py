import logging
from functions import *
from datetime import datetime


# Configure logging
logging.basicConfig(
    filename='etl_process.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )


class ETL:
    playlist_ids = None

    def __init__(self):
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)

            if not config or 'playlist_ids' not in config:
                raise KeyError("Missing required key 'playlist_ids' in config.json")

            self.playlist_ids = config['playlist_ids']
            logging.info("Successfully loaded playlist IDs from config.")

        except json.JSONDecodeError:
            logging.error("The config.json file contains invalid JSON.")
            raise ValueError("The config.json file contains invalid JSON. Please check the file format.")
        except KeyError as e:
            logging.error(str(e))
            raise e

    def _extract(self) -> pd.DataFrame:
        try:
            logging.info("Starting extraction. Please ensure network connectivity. This may take a while...")
            dataframes = []
            for pid in self.playlist_ids:
                logging.info(f"Extracting comments for playlist ID: {pid}")
                playlist_comments = extract_playlist_comments(playlistId=pid)
                dataframes.append(playlist_comments)
                logging.info(f"Finished extracting comments for playlist ID: {pid}")

            logging.info("Extraction complete.")
            return pd.concat(dataframes, axis=0).reset_index(drop=True)
        except Exception as e:
            logging.error(f"An error occurred during extraction: {str(e)}")
            raise Exception(str(e))

    def _transform(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            logging.info("Starting data transformation.")
            clean_date(data)
            clean_text(data)
            clean_text_sentiment_analysis(data)
            logging.info("Data transformation complete.")
            return data
        except Exception as e:
            logging.error(f"An error occurred during transformation: {str(e)}")
            raise e

    def _load(self, data: pd.DataFrame):
        try:
            logging.info("Loading data...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"extracted_data_{timestamp}.csv"

            data.to_csv(filename, index=False)
            logging.info(f"Data saved to {filename}")
        except Exception as e:
            logging.error(f"An error occurred during loading: {str(e)}")
            raise e

    def start(self):
        logging.info("ETL process started.")
        try:
            data = self._extract()
            processed = self._transform(data)
            self._load(processed)
            logging.info("ETL process completed successfully.")
        except Exception as e:
            logging.error(f"ETL process failed: {str(e)}")
            raise e
