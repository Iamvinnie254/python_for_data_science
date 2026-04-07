import pandas as pd
from data_ingestion import create_db_engine, query_data, read_from_web_CSV
import logging

### START FUNCTION

class FieldDataProcessor:
    """
    A data processing class for handling agricultural field data.

    This class is responsible for:
    - Ingesting data from a SQL database
    - Cleaning and transforming the dataset
    - Applying corrections to values
    - Mapping weather station data from an external CSV source
    """

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initializes the FieldDataProcessor with configuration parameters.

        Args:
            config_params (dict): Dictionary containing configuration settings:
                - db_path (str): Path to the database
                - sql_query (str): SQL query to retrieve data
                - columns_to_rename (dict): Columns to swap
                - values_to_rename (dict): Incorrect-to-correct value mappings
                - weather_mapping_csv (str): URL/path to weather mapping CSV
            logging_level (str): Logging verbosity level (DEBUG, INFO, NONE)
        """
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_map_data = config_params['weather_mapping_csv']

        self.initialize_logging(logging_level)

        # Initialize storage attributes
        self.df = None
        self.engine = None
        
    def initialize_logging(self, logging_level):
        """
        Configures logging for the class instance.

        Args:
            logging_level (str): Logging level to control output verbosity.
                                 Options: DEBUG, INFO, NONE.
        """
        logger_name = __name__ + ".FieldDataProcessor"
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

    def ingest_sql_data(self):
        """
        Connects to the database and retrieves data using the provided SQL query.

        Returns:
            pandas.DataFrame: The loaded dataset.
        """
        self.engine = create_db_engine(self.db_path)
        self.df = query_data(self.engine, self.sql_query)
        self.logger.info("Sucessfully loaded data.")
        return self.df
    
    def rename_columns(self):
        """
        Swaps the names of two columns in the DataFrame to correct mislabeling.

        The columns to swap are defined in the configuration dictionary.
        A temporary column name is used to avoid naming conflicts.
        """
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]

        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"
            
        self.logger.info(f"Swapped columns: {column1} with {column2}")

        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})  
    
    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Applies data cleaning transformations to the DataFrame.

        - Converts values in the specified numeric column to absolute values
        - Corrects inconsistent categorical values using a mapping dictionary

        Args:
            column_name (str): Column containing categorical values to correct.
            abs_column (str): Column whose values should be made absolute.
        """
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(
            lambda crop: self.values_to_rename.get(crop, crop)
        )

    def weather_station_mapping(self):
        """
        Loads weather station mapping data from an external CSV source.

        If the main DataFrame is already loaded, merges the weather mapping
        into it using 'Field_ID' as the key and removes any unnecessary columns.

        Returns:
            pandas.DataFrame: The weather mapping DataFrame.
        """
        return read_from_web_CSV(self.weather_map_data)

        if self.df is not None:
            self.df = self.df.merge(weather_map_df, on='Field_ID', how='left')
            self.df.drop(columns="Unnamed: 0", errors="ignore", inplace=True)
        
        return weather_map_df
    

    def process(self):
        """
        Executes the full data processing pipeline in sequence.

        Steps:
        1. Load data from SQL database
        2. Rename incorrectly labeled columns
        3. Apply value corrections
        4. Load and merge weather station mapping data

        Returns:
            None: The processed DataFrame is stored in self.df
        """
        self.ingest_sql_data()
        self.rename_columns()
        self.apply_corrections()
        weather_map_df = self.weather_station_mapping()
        self.df = self.df.merge(weather_map_df, on='Field_ID', how='left')
        self.df.drop(columns="Unnamed: 0", errors="ignore", inplace=True)

### END FUNCTION