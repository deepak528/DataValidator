import json
import duckdb
from src import myClasses
from src import ValidationMethods
from src import logging

# Global Variables
connections = {}
testCases = {}


        
# Define all connections; in future, read from config file
def readConnections():
    print ("Reading Connections...")
    with open("tests\\connections.json") as json_file:
        json_data = json.load(json_file)
        #print(json_data)
        
    for connName in json_data:
        myConnection = myClasses.Connection(connName, json_data[connName]["type"], json_data[connName]["path"])
        connections[connName] = myConnection
    
    
    print ("Connections created: " + str(len(connections)))
    print ("")

# Read all test cases; in future, read from config file
def readTestCases():
    print ("Reading Test Cases...")
    
    with open("tests\\testcases.json") as json_file:
        json_data = json.load(json_file)

    # read test case details in JSON        
    for tcName in json_data:
        myTestCaseJSON = json_data[tcName]
        
        # create object
        myTestCase = myClasses.TestCase(str(tcName),
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
        if myTestCase.validationType == myClasses.TESTCASE_TYPE_COUNT:
            bPassFail = ValidationMethods.validateCount (myTestCase, connections)
        elif myTestCase.validationType == myClasses.TESTCASE_TYPE_VALUE:
            bPassFail = ValidationMethods.validateValue (myTestCase, connections)
        
        # convert execution result to pass fail
        myTestCase.PassFail = "Pass" if bPassFail == True else "Fail"    
    
        print ("Result: " + myTestCase.PassFail)
            
