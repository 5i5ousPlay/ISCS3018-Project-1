import os
import logging
import pandas as pd
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
    output_dir = "data"  # default output directory

    def __init__(self):
        """
        Initializes the ETL class by loading configuration from config.json.

        Checks for the presence of 'playlist_ids' and 'output_directory' keys.
        Creates the output directory if it does not exist and checks if it is writable.
        Raises exceptions for invalid JSON or missing keys.
        """
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)

            # check if playlist ids are specified in config file
            if not config or 'playlist_ids' not in config:
                raise KeyError("Missing required key 'playlist_ids' in config.json")

            # set output directory if specified in the config
            if 'output_directory' in config:
                self.output_dir = config['output_directory']

            # create output directory if it does not exist
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)
                logging.info(f"Directory '{self.output_dir}' created.")

            # check if output directory is writable
            if not os.access(self.output_dir, os.W_OK):
                raise PermissionError(f"Write permission denied for the directory '{self.output_dir}'.")

            self.playlist_ids = config['playlist_ids']
            logging.info("Successfully loaded playlist IDs from config.")

        except json.JSONDecodeError:
            logging.error("The config.json file contains invalid JSON.")
            raise ValueError("The config.json file contains invalid JSON. Please check the file format.")
        except KeyError as e:
            logging.error(str(e))
            raise e
        except Exception as e:
            logging.error(f"An error occurred during initialization: {str(e)}")
            raise e

    def _extract(self) -> pd.DataFrame:
        """
        Extracts comments from YouTube playlists specified in the playlist_ids.
        """
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
        """
        Transforms the extracted data by cleaning and processing it.
        """
        try:
            logging.info("Starting data transformation.")

            # Cleaning steps
            clean_date(data)
            clean_text(data)
            clean_text_sentiment_analysis(data)

            # Replace empty strings or None with np.nan
            data.replace({'cleaned_text': {'': pd.NA}}, inplace=True)
            data.replace({'cleaned_text_sentiment': {'': pd.NA}}, inplace=True)

            # drop rows with NaN values
            original_row_count = len(data)
            data = data.dropna()
            new_row_count = len(data)
            logging.info(f"Dropped {original_row_count - new_row_count} rows out of {original_row_count}")

            logging.info("Data transformation complete.")
            return data
        except Exception as e:
            logging.error(f"An error occurred during transformation: {str(e)}")
            raise e

    def _load(self, data: pd.DataFrame):
        """
        Loads the transformed data into a CSV file.
        """
        try:
            logging.info("Loading data...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"extracted_data_{timestamp}.csv"
            output_file = os.path.join(self.output_dir, filename)

            data.to_csv(output_file, index=False)
            logging.info(f"Data saved to {output_file}")
        except Exception as e:
            logging.error(f"An error occurred during loading: {str(e)}")
            raise e

    def start(self):
        """
        Starts the ETL process by executing the extract, transform, and load steps.
        """
        logging.info("ETL process started.")
        try:
            data = self._extract()
            processed = self._transform(data)
            self._load(processed)
            logging.info("ETL process completed successfully.")
        except Exception as e:
            logging.error(f"ETL process failed: {str(e)}")
            raise e
