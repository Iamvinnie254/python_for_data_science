# These are the imports we're going to use in the weather data processing module
import re
import numpy as np
import pandas as pd
import logging
from data_ingestion import read_from_web_CSV

patterns = {
    'Rainfall': r'(\d+(\.\d+)?)\s?mm',
     'Temperature': r'(\d+(\.\d+)?)\s?C',
    'Pollution_level': r'=\s*(-?\d+(\.\d+)?)|Pollution at \s*(-?\d+(\.\d+)?)'
    }


### START FUNCTION 

class WeatherDataProcessor:
    """
    A class for processing weather station data.

    This class handles:
    - Loading weather station data from a CSV source
    - Extracting measurements from text messages using regex patterns
    - Structuring extracted data into usable columns
    - Computing summary statistics such as mean values
    """

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initializes the WeatherDataProcessor with configuration parameters.

        Args:
            config_params (dict): Dictionary containing:
                - weather_csv_path (str): Path/URL to weather data CSV
                - regex_patterns (dict): Dictionary of measurement patterns
            logging_level (str): Logging level (DEBUG, INFO, NONE)
        """
        self.weather_station_data = config_params['weather_csv_path']
        self.patterns = config_params['regex_patterns']
        self.weather_df = None  # Initialize weather_df as None or as an empty DataFrame
        self.initialize_logging(logging_level)

    def initialize_logging(self, logging_level):
        """
        Sets up logging configuration for the class instance.

        Args:
            logging_level (str): Logging verbosity level.
        """
        logger_name = __name__ + ".WeatherDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False

        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO

        self.logger.setLevel(log_level)

        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def weather_station_mapping(self):
        """
        Loads weather station data from a CSV source and assigns it to the class DataFrame.

        This method initializes `self.weather_df` using data retrieved from a web source.
        """
        self.weather_df = read_from_web_CSV(self.weather_station_data)
        self.logger.info("Successfully loaded weather station data from the web.") 
        # Here, you can apply any initial transformations to self.weather_df if necessary.

    
    def extract_measurement(self, message):
        """
        Extracts a measurement type and its value from a message string using regex patterns.

        Args:
            message (str): Input message containing weather information.

        Returns:
            tuple:
                - measurement type (str or None)
                - extracted value (float or None)
        """
        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
            if match:
                self.logger.debug(f"Measurement extracted: {key}")
                return key, float(next((x for x in match.groups() if x is not None)))
        self.logger.debug("No measurement match found.")
        return None, None

    def process_messages(self):
        """
        Processes all messages in the dataset to extract measurements and values.

        Adds two new columns to `self.weather_df`:
            - Measurement
            - Value

        Returns:
            pandas.DataFrame: Updated DataFrame with extracted data.
        """
        if self.weather_df is not None:
            result = self.weather_df['Message'].apply(self.extract_measurement)
            self.weather_df['Measurement'], self.weather_df['Value'] = zip(*result)
            self.logger.info("Messages processed and measurements extracted.")
        else:
            self.logger.warning("weather_df is not initialized, skipping message processing.")
        return self.weather_df

    def calculate_means(self):
        """
        Calculates mean values of measurements grouped by weather station and measurement type.

        Returns:
            pandas.DataFrame or None:
                Pivoted table of mean values if data exists, otherwise None.
        """
        if self.weather_df is not None:
            means = self.weather_df.groupby(by=['Weather_station_ID', 'Measurement'])['Value'].mean()
            self.logger.info("Mean values calculated.")
            return means.unstack()
        else:
            self.logger.warning("weather_df is not initialized, cannot calculate means.")
            return None
    
    def process(self):
        """
        Executes the weather data processing pipeline.

        Steps:
        1. Load weather station data
        2. Extract measurements from messages

        Returns:
            None: Processed data is stored in `self.weather_df`
        """
        self.weather_station_mapping()
        self.process_messages()
        self.logger.info("Data processing completed.")

### END FUNCTION