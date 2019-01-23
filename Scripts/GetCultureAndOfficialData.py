import pandas as pd
import pyodbc as dbc
import time
import logging

# Log messages to the console.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logFormatter = logging.Formatter("%(asctime)s:%(name)s:%(message)s")
streamHandler = logging.StreamHandler()
streamHandler.setFormatter(logFormatter)
logger.addHandler(streamHandler)


def sql_server_connection():
    """Create a connection to the database."""
    server = "US31SQLP001"
    db = "eLimsMilkReportExports"
    uid = "sa"
    pwd = "Eurofins@123"
    connection = dbc.connect(driver='{SQL Server}', host=server,
                             database=db, user=uid, password=pwd)
    return connection


def query_to_df(query):
    """Executes sql query then imports the data into a dataframe"""
    conn = sql_server_connection()
    dataFrame = pd.io.sql.read_sql(query, conn)
    conn.commit()
    conn.close()
    return dataFrame


def get_file_name(path, namePrefix):
    """Create a unique file name."""
    timeStr = time.strftime("%Y%m%d-%H%M%S")
    fileName = "{0}{1}{2}{3}".format(path, namePrefix, timeStr, ".CSV")
    return fileName


def create_csv_file(dataFrame, filePath, fileNamePrefix):
    """Creates a csv file containing data from a dataframe"""
    if not dataFrame.empty:
        dataFrame.to_csv(get_file_name(filePath, fileNamePrefix), index=False)
    return dataFrame


def official_data_to_csv():
    """Get Officials data to a csv file"""
    filePath = "//us31apva/m-drive/Input/"
    query = "EXECUTE dbo.GetOfficialsData"
    df = query_to_df(query)
    logger.info("Query:{0} executed succesfully".format(query))
    create_csv_file(df, filePath, "OfficialsResults")
    logger.info("Officials data file created")


def ampi_culture_data_to_csv():
    """Get AMPI cultures data to a csv file."""
    filePath = "//US31SQLP001/CulturesData/"
    query = "EXECUTE dbo.GetCultureData 'AMPI' "
    df = query_to_df(query)
    logger.info("Query:{0} executed succesfully".format(query))
    create_csv_file(df, filePath, "AMPICultureResults")
    logger.info("AMPI cultures data file created")


def culture_data_to_csv():
    """Get cultures data for all clients except AMPI."""
    filePath = "//US31SQLP001/CulturesData/"
    query = "EXECUTE dbo.GetCultureData 'ALL' "
    df = query_to_df(query)
    logger.info("Query:{0} executed succesfully".format(query))
    create_csv_file(df, filePath, "CultureResults")
    logger.info("Cultures data file created")


official_data_to_csv()
ampi_culture_data_to_csv()
culture_data_to_csv()
