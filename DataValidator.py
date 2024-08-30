import duckdb
import ValidationEngine


# Global Variables
#connections = {}
#testCases = {}

def main():
    # boot up
    ValidationEngine.readConnections()
    ValidationEngine.readTestCases()
    ValidationEngine.executeTestCases()
    
    
main()

#result = duckdb.sql("CREATE TABLE ontime AS SELECT * FROM 'flights.csv';