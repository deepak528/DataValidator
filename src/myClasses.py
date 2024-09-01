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