import json
import duckdb

# Global Variables
connections = {}
testCases = {}

# Constants
TESTCASE_TYPE_COUNT = "count"
TESTCASE_TYPE_VALUE = "value"
TESTCASE_TYPE_COMPARE = "compare"
TESTCASE_TYPE_RECON = "Reconcillation"
CONNECTION_CSV = "CSVFile"
CONNECTION_SQL = "SQLServer"


# File or Database Connection
class Connection:
   def __init__ (self, name, type_, path):
        self.name = name
        self.type_ = type_
        self.path = path
        
# Test Case object
class TestCase:
    def __init__(self, name, validationType, sourceConnection, sourceSQL, targetConnection, targetSQL):
        self.name = name
        self.validationType = validationType #checksum
        self.sourceConnection = sourceConnection
        self.sourceSQL = sourceSQL
        self.targetConnection = targetConnection
        self.targetSQL = targetSQL
        
# Define all connections; in future, read from config file
def readConnections():
    print ("Reading Connections...")
    with open("C:\\Users\\deepa\\OneDrive\\Documents\\GitHub\\DataValidator\\connections.json") as json_file:
        json_data = json.load(json_file)
        #print(json_data)
        
    for connName in json_data:
        myConnection = Connection(connName, json_data[connName]["type"], json_data[connName]["path"])
        connections[connName] = myConnection
    
    
    print ("Connections created: " + str(len(connections)))
    print ("")

# Read all test cases; in future, read from config file
def readTestCases():
    print ("Reading Test Cases...")
    
    with open("C:\\Users\\deepa\\OneDrive\\Documents\\GitHub\\DataValidator\\testcases.json") as json_file:
        json_data = json.load(json_file)

    # read test case details in JSON        
    for tcName in json_data:
        myTestCaseJSON = json_data[tcName]
        
        # create object
        myTestCase = TestCase(str(tcName),
                              myTestCaseJSON["type"],
                              myTestCaseJSON["sourceConnection"],
                              myTestCaseJSON["sourceSQL"],
                              myTestCaseJSON["targetConnection"],
                              myTestCaseJSON["targetSQL"])
        
        # add to test case collection
        testCases[tcName] = myTestCase

    print ("Test Cases: " + str(len(testCases)))
    print ("")
    
    
def executeTestCases():
    print ("Executing Test Cases...")
    
    count = len(testCases)
    
    print ("Total Test Cases: " + str(count))
    print ("")
    
    if count == 0:
        print ("No test cases to execute.")
        return -1
    
    for testCaseName in testCases:
        print ("Running Test Case: " + testCaseName)
        
        myTestCase = testCases[testCaseName]
        
        # execute test case
        if myTestCase.validationType == TESTCASE_TYPE_COUNT:
            bPassFail = validateCount (myTestCase)
        elif myTestCase.validationType == TESTCASE_TYPE_VALUE:
            bPassFail = validateValue (myTestCase)
        
        # convert execution result to pass fail
        myTestCase.PassFail = "Pass" if bPassFail == True else "Fail"    
    
        print ("Result: " + myTestCase.PassFail)
            
def validateCount (tc: TestCase):
    #print ("Running Test Case: " + tc.name)
    
    sourceConnection = tc.sourceConnection
    
    if sourceConnection not in connections:
        logMessage ("Invalid connection name: " + sourceConnection)
        
    CSVPath = connections[sourceConnection].path
    sql = tc.sourceSQL
    targetExpression = tc.targetSQL
    
    logMessage ("CSV Path: " + CSVPath)
    logMessage ("SQL: " + sql)
        
    # connect to CSV File
    logMessage ("Creating Table from CSV File")
    
    # run SQL and store count in a variable
    duckdb.sql("CREATE TABLE myTable AS SELECT * FROM '" + CSVPath + "'")
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
    
    if expressionResult == True:
        return True
    else:
        return False

def validateValue (tc: TestCase):
    #print ("Running Test Case: " + tc.name)
    
    sourceConnection = tc.sourceConnection
    
    if sourceConnection not in connections:
        logMessage ("Invalid connection name: " + sourceConnection)
        
    CSVPath = connections[sourceConnection].path
    sql = tc.sourceSQL
    targetExpression = tc.targetSQL
    
    logMessage ("Test Case Type: " + TESTCASE_TYPE_VALUE)
    logMessage ("Source Path: " + CSVPath)
    logMessage ("SQL: " + sql)
        
    # connect to CSV File
    logMessage ("Creating Table from CSV File")
    
    # run SQL and store count in a variable
    duckdb.sql("CREATE TABLE myTable AS SELECT * FROM '" + CSVPath + "'")
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
    
    if expressionResult == True:
        return True
    else:
        return False

def logMessage (message):
    print ("  --> " + message)