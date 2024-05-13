import logging

import pandas as pd
from zenml import step


class IngestData:
    """
    Data ingestion class which ingests data from the source and returns a DataFrame.
    """

    def __init__(self) -> None:
        """Initialize the data ingestion class."""
        pass

    def get_data(self) -> pd.DataFrame:
        """Get csv data"""
        df = pd.read_csv("../data/olist_customers_dataset.csv")
        return df


@step
def ingest_data() -> pd.DataFrame:
    """
    Args:
        None
    Returns:
        df: pd.DataFrame
    """
    try:
        ingesting_data = IngestData()
        df = ingesting_data.get_data()
        return df
    except Exception as e:
        logging.error('Error ingesting data %s', e)
        raise e
