import duckdb
from src import myClasses
from src import logging
from pathlib import Path
import pandas

def createTableFromConnection(strTableName, strConnection, connections):
    
    # check if connection is valid
    if strConnection not in connections:
        logging.logMessage ("Invalid connection name: " + strConnection)
        return False

    # get connection details
    strConnectionType = connections[strConnection].type_
    strConnectionPath = connections[strConnection].path

    if strConnectionType == myClasses.CONNECTION_CSV:
        my_file = Path(strConnectionPath)
        
        #check if file exists
        if my_file.is_file() == False:
            logging.logMessage ("Files does not exist: " + strConnectionPath)
            return False

        logging.logMessage ("CSV Path: " + strConnectionPath)

        # connect to CSV File
        logging.logMessage ("Creating Table from CSV File")
        titanic = pandas.read_csv(strConnectionPath)
        print (titanic.head(8))
        return False

        #duckdb.sql("CREATE TABLE " + strTableName + " AS SELECT * FROM '" + strConnectionPath + "'")
        return True
    elif strConnectionType == myClasses.CONNECTION_SQL:
        # SQL Server isn't supported yet
        logging.logMessage ("Invalid Connection Type: " + strConnectionType)
        return False
    else:
        # Unsupported connection type
        logging.logMessage ("Invalid Connection Type: " + strConnectionType)
        return False


###########################################################################
# Purpose:  Function to validate a single record count value return from a SQL
# Example SQL:  Select count(*) from table1
# Example Expression: count > 2000 
###########################################################################
def validateCount (tc: myClasses.TestCase, connections):

    # create Connection
    if createTableFromConnection ("myTable", tc.sourceConnection, connections) == False:
        return False
    
    # run SQL and store count in a variable
    sql = tc.sourceSQL
    targetExpression = tc.targetSQL
    logging.logMessage ("SQL: " + sql)

    response = duckdb.sql(sql).fetchall()
    response = str(response[0][0])
    
    # evalute expression
    targetExpression = targetExpression.replace("result", response)
    logging.logMessage ("Expression: " + targetExpression)
    expressionResult = eval(targetExpression)    
    
    logging.logMessage ("source SQL response: " + response)
    logging.logMessage ("evalutation:" + str(expressionResult))
    
    logging.logMessage ("Dropping Table")
    duckdb.sql("DROP TABLE myTable")
    
    # return response
    return expressionResult

def validateValue (tc: myClasses.TestCase, connections):

    # create Connection
    if createTableFromConnection ("myTable", tc.sourceConnection, connections) == False:
        return False
    
    # run SQL and store count in a variable
    sql = tc.sourceSQL
    targetExpression = tc.targetSQL
    logging.logMessage ("SQL: " + sql)

    response = duckdb.sql(sql).fetchall()
    response = str(response[0][0])
    
    # evalute expression
    targetExpression = targetExpression.replace("result", response)
    logging.logMessage ("Expression: " + targetExpression)
    expressionResult = eval(targetExpression)    
    
    logging.logMessage ("source SQL response: " + response)
    logging.logMessage ("evalutation:" + str(expressionResult))
    
    logging.logMessage ("Dropping Table")
    duckdb.sql("DROP TABLE myTable")
    
    # return response
    return expressionResult