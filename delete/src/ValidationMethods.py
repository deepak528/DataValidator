import duckdb
#from src import ValidationEngine
#from src import TestCasesLib
#from src import logging
from pathlib import Path
import pandas
import pyodbc
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import sqlalchemy as sa

# Constants
CONNECTION_CSV = "CSVFile"
CONNECTION_SQL = "SQLServer"

def logMessage (message):
    print ("  --> " + message)

def createTableFromConnection(strTableName, strConnection, strSQL, connections):
    # check if connection is valid
    if strConnection not in connections:
        logMessage ("Invalid connection name: " + strConnection)
        return False

    # get connection details
    strConnectionType = connections[strConnection].type_
    strConnectionPath = connections[strConnection].path
    
    if strConnectionType == CONNECTION_CSV:
        logMessage ("Creating Table from CSV File...")
        
        response = createTableFromCSV(strTableName, strConnectionPath, strSQL)

        if response != False:
            logMessage ("Table Created: " + strTableName)

        return response

    elif strConnectionType == CONNECTION_SQL:
        logMessage ("Creating Table from SQL Database...")
        
        response = createTableFromSQLDB(strTableName, strConnectionPath, strSQL)

        if response != False:
            logMessage ("Table Created: " + strTableName)

        return response

def createTableFromCSV(strTableName, strConnectionPath, strSQL):
    
    my_file = Path(strConnectionPath)
    
    #check if file exists
    if my_file.is_file() == False:
        logMessage ("File does not exist: " + strConnectionPath)
        return False
    else:
        logMessage ("CSV Path: " + strConnectionPath)
        logMessage ("SQL: " + strSQL)

    # connect to CSV File
    df = pandas.read_csv(strConnectionPath)
    duckdb.sql("CREATE TABLE " + strTableName + " AS SELECT * FROM df")

    logMessage ("Record Count: " + str(df.count()))

    # run SQL and store count in a variable
    response = duckdb.sql(strSQL).fetchall()

    return response

def createTableFromSQLDB(strTableName, strConnectionPath, strSQL):
    
    logMessage ("Creating Table from SQL Database")

    # create connection string
    connection_string = "DRIVER={SQL Server};" + strConnectionPath
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

    # create connection
    engine = create_engine(connection_url)

    # run SQL
    try:
        with engine.begin() as conn:
            df = pandas.read_sql_query(sa.text(strSQL), conn)
            #break
    except (RuntimeError, TypeError, NameError):
        logMessage ("Error running database SQL")
        return False

    logMessage ("Records Read: " + str(df.count()))

    # transfer data to duckdb table
    duckdb.sql("CREATE TABLE " + strTableName + " AS SELECT * FROM df")

    # Run SQL
    response = duckdb.sql("SELECT * FROM " + strTableName).fetchall()


    # run SQL and store count in a variable
    #sql = tc.sourceSQL
    #targetExpression = tc.targetSQL
    #logMessage ("SQL: " + strSQL)

    #response = duckdb.sql(strSQL).fetchall()

    #duckdb.sql("CREATE TABLE " + strTableName + " AS SELECT * FROM '" + strConnectionPath + "'")
    return response



###########################################################################
# Purpose:  Function to validate a single record count value return from a SQL
# Example SQL:  Select count(*) from table1
# Example Expression: count > 2000 
###########################################################################
def validateCount (tc, connections):

    # create Connection
    if createTableFromConnection ("myTable", tc.sourceConnection, connections) == False:
        return False
    
    # run SQL and store count in a variable
    sql = tc.sourceSQL
    targetExpression = tc.targetSQL
    logMessage ("SQL: " + sql)

    response = duckdb.sql(sql).fetchall()
    response = str(response[0][0])
    
    # evalute expression
    targetExpression = targetExpression.replace("result", response)
    logMessage ("Expression: " + targetExpression)
    expressionResult = eval(targetExpression)    
    
    logMessage ("source SQL response: " + response)
    logMessage ("evalutation:" + str(expressionResult))
    
    logMessage ("Dropping Table")
    duckdb.sql("DROP TABLE myTable")
    
    # return response
    return expressionResult

def validateValue (tc, connections):

    # create Connection
    if createTableFromConnection ("myTable", tc.sourceConnection, connections) == False:
        return False
    
    # run SQL and store count in a variable
    sql = tc.sourceSQL
    targetExpression = tc.targetSQL
    logMessage ("SQL: " + sql)

    response = duckdb.sql(sql).fetchall()
    response = str(response[0][0])
    
    # evalute expression
    targetExpression = targetExpression.replace("result", response)
    logMessage ("Expression: " + targetExpression)
    expressionResult = eval(targetExpression)    
    
    logMessage ("source SQL response: " + response)
    logMessage ("evalutation:" + str(expressionResult))
    
    logMessage ("Dropping Table")
    duckdb.sql("DROP TABLE myTable")
    
    # return response
    return expressionResult



def Validate_CompareSource2Target (tc, connections):

    logMessage ("Running comparison...")

    # create Source Connection
    response1 = createTableFromConnection ("mySourceTable", tc.sourceConnection, tc.sourceSQL, connections)
    
    if response1 == False:
        logMessage ("Cannot create Source Connection")
        return False
    else:
        logMessage ("Created Source Table")

    # create Source Connection
    response2 = createTableFromConnection ("myTargetTable", tc.targetConnection, tc.targetSQL, connections)

    if response2 == False:
        logMessage ("Cannot create Target Connection")
        return False
    else:
        logMessage ("Created Target Table")
    
    # drop tables
    duckdb.sql ("DROP TABLE mySourceTable")
    duckdb.sql ("DROP TABLE myTargetTable")

    return True