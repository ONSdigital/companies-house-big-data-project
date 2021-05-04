"""File utilities functions."""
# import libraries
import pandas as pd
import os 
from pathlib import Path
from typing import Optional


def df_to_gbq(df: pd.DataFrame,
              project_id: str,
              database: str,
              table: str,
              schema: Optional[list] = None,
              key: Optional[str] = None):
    """
    Export a pandas dataframe to a big query table.
    Parameters
    ------------
    df
        pandas dataframe
    project_id
        the GCP project id
    table
        name of new Big Query table
    database
        name of the Big Query database
    schema
        manual schema of dataframe (default = None)
    key
        filepath of the authentication key (json) file (default = None)
    Returns
    ------------
    None
    """
    table_id = database + '.' + table

    df.to_gbq(table_id, project_id=project_id, table_schema=schema,
              credentials=key)

    return None



def read_bq_table(table_id: str,
                  project_id: str,
                  col_query: str = """SELECT * FROM """,
                  cond_query: str = "",
                  bqstorage: bool = False,
                  key: Optional[str] = None):
    """
    Query and import a big query table as a pandas dataframe.
    Parameters
    -----------
    table_id
        Biq Query table name (project.database.table)
    project_id
        the GCP project id
    col_query
        SQL SELECT {columns} FROM query, wrapped in
        single triple quotes
    cond_query
        SQL query of the conditions / operations to
                          preform on the Big Query table
    bqstorage
        Option to use big query API at increased cost (default = False)
    key
        filepath of the authentication key (json) file (default = None)
    Returns
    -----------
    df
        resultant dataframe of the SQL operation
        on the big query table
    """
    sql_query = col_query + "`{}`".format(table_id) + cond_query
    df = pd.read_gbq(sql_query, project_id=project_id,
                     dialect='standard', use_bqstorage_api=bqstorage,
                     credentials=key, progress_bar_type='tqdm')
    
    return df
