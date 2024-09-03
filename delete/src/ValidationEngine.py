import json
from src import logging
from src import ValidationMethods
import pandas
import duckdb
import pyodbc
from pathlib import Path
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import sqlalchemy as sa

######################################################################
# CONSTANTS
######################################################################
CONNECTION_CSV = "CSVFile"
CONNECTION_SQL = "SQLServer"
TESTCASE_TYPE_COUNT = "count"
TESTCASE_TYPE_VALUE = "value"
TESTCASE_TYPE_COMPARE = "compare"

# global variable
connections = {}

######################################################################
# CLASSES
######################################################################

# File or Database Connection
class Connection:
   def __init__ (self, name, type_, path):
        self.name = name
        self.type_ = type_
        self.path = path
        
# Global Variables
testCases = {}

# Test Case object
class TestCase:
    def __init__(self, name, validationType, sourceConnection, sourceSQL, targetConnection, targetSQL):
        self.name = name
        self.validationType = validationType #checksum
        self.sourceConnection = sourceConnection
        self.sourceSQL = sourceSQL
        self.targetConnection = targetConnection
        self.targetSQL = targetSQL


######################################################################
# Functions to read Test Cases
######################################################################


# Define all connections; in future, read from config file
def readConnections():
    # Temporary Variables

    print ("Reading Connections...")
    with open("tests\\connections.json") as json_file:
        json_data = json.load(json_file)
        #print(json_data)
        
    for connName in json_data:
        myConnection = Connection(connName, json_data[connName]["type"], json_data[connName]["path"])
        connections[connName] = myConnection
    
    
    print ("Connections created: " + str(len(connections)))
    print ("")

    return connections

# Read all test cases; in future, read from config file
def readTestCases():
    print ("Reading Test Cases...")
    
    with open("tests\\testcases.json") as json_file:
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

######################################################################
# Functions to Connect to Flat File & Database
######################################################################



    
    
def executeTestCases(connections):
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

        logging.logMessage ("Type: " + myTestCase.validationType)
        
        # execute test case
        if myTestCase.validationType == TESTCASE_TYPE_COUNT:
            bPassFail = False #ValidationMethods.validateCount (myTestCase, connections)
        elif myTestCase.validationType == TESTCASE_TYPE_VALUE:
            bPassFail = False #ValidationMethods.validateValue (myTestCase, connections)
        elif myTestCase.validationType == TESTCASE_TYPE_COMPARE:
            bPassFail = ValidationMethods.Validate_CompareSource2Target(myTestCase, connections)
        
        # convert execution result to pass fail
        myTestCase.PassFail = "Pass" if bPassFail == True else "Fail"    
    
        print ("Result: " + myTestCase.PassFail)
            
