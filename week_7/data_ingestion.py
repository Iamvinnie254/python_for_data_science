from sqlalchemy import create_engine, text
import logging
import pandas as pd
# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

### START FUNCTION

"""
data_ingestion.py

This module provides utility functions for ingesting data from various sources,
including relational databases and web-hosted CSV files. It includes functionality
to create database engine connections, execute SQL queries, and read CSV data
directly from URLs.

Dependencies:
- pandas
- sqlalchemy

Logging is used to track the success and failure of operations.
"""


def create_db_engine(db_path):
    """
    Create and validate a SQLAlchemy database engine.

    This function initializes a database engine using the provided database path
    and tests the connection to ensure it is valid.

    Parameters:
    -----------
    db_path : str
        The database connection string (e.g., 'sqlite:///example.db').

    Returns:
    --------
    engine : sqlalchemy.engine.Engine
        A SQLAlchemy engine object if the connection is successful.

    Raises:
    -------
    ImportError
        If SQLAlchemy is not installed.
    Exception
        If the engine creation or connection fails.
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine # Return the engine object if it all works well
    except ImportError: #If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:# If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e
    
def query_data(engine, sql_query):
    """
    Execute a SQL query and return the results as a Pandas DataFrame.

    This function connects to the provided database engine, executes the given
    SQL query, and loads the result into a DataFrame.

    Parameters:
    -----------
    engine : sqlalchemy.engine.Engine
        A valid SQLAlchemy engine object.
    sql_query : str
        The SQL query to be executed.

    Returns:
    --------
    df : pandas.DataFrame
        A DataFrame containing the query results.

    Raises:
    -------
    ValueError
        If the query returns an empty DataFrame.
    Exception
        If an error occurs during query execution.
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e
    
def read_from_web_CSV(URL):
    """
    Read a CSV file from a web URL into a Pandas DataFrame.

    This function fetches a CSV file from the specified URL and loads it into
    a DataFrame for further processing.

    Parameters:
    -----------
    URL : str
        The URL pointing to the CSV file.

    Returns:
    --------
    df : pandas.DataFrame
        A DataFrame containing the data from the CSV file.

    Raises:
    -------
    pandas.errors.EmptyDataError
        If the URL does not contain valid CSV data.
    Exception
        If the file cannot be accessed or read.
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e
    
### END FUNCTION