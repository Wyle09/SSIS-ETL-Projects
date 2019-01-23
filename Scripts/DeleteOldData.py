import pyodbc 

def sql_server_connection():
    """Creates a connection to the database."""    
    server = "US31SQLP001"
    db = "eLimsMilkReportExports"
    uid = "sa"
    pwd = "Eurofins@123"
    connection = pyodbc.connect(driver = '{SQL Server}', host = server, 
                             database = db, user= uid, password = pwd)
    
    return connection

def execute_query(query):
    """Executes and commits queries, use for 'Insert', 'Delete', 'Update'."""    
    conn = sql_server_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()

def delete_data_staging():
    """Delete old data from the staging table (M2MFileData)."""    
    query = """
            DELETE M2MFileData
            WHERE CAST(RecordReceived AS DATE) < DATEADD(DAY, -40, GETDATE())
            OR RecordReceived IS NULL; 
            """
    execute_query(query)
    
def delete_data_transactions(): 
    """Delete old data from the ReportTransactions table."""    
    query = """
            DELETE ReportTransactions
            WHERE dbo.UTCToLocalTime(LastUpdatedDate, 'MV') < DATEADD(DAY, -40, GETDATE())
            """
    execute_query(query)
        
delete_data_staging()
delete_data_transactions()