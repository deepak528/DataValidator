import duckdb
from src import ValidationEngine
import pandas as pd
import pyodbc
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import sqlalchemy as sa
    
# Global Variables

#connections = {}
#testCases = {}

def main():
    # boot up
    ValidationEngine.readConnections()
    ValidationEngine.readTestCases()
    ValidationEngine.executeTestCases()
    
def test():
    #data = pandas.read_csv("data\\Electric_Vehicle_Population_Data.csv") 
    cnxn = pyodbc.connect("Driver={SQL Server};"
                      "Server=Deepak-Laptop;"
                      "Database=MemberPortal;"
                      "Trusted_Connection=yes;")
    
    connection_string = "DRIVER={SQL Server};SERVER=Deepak-Laptop;DATABASE=MemberPortal;Trusted_Connection=yes"
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

    engine = create_engine(connection_url)

    with engine.begin() as conn:
        df = pd.read_sql_query(sa.text("SELECT count(*) FROM dbo.Electric_Vehicle_Population_Data"), conn)

    duckdb.sql("CREATE TABLE my_table AS SELECT * FROM df")

    print (df.head(10))

test()

#result = duckdb.sql("CREATE TABLE ontime AS SELECT * FROM 'flights.csv';