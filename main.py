import ValidationEngine as validator

# constants
csv_file_path = "data\\Electric_Vehicle_Population_Data.csv"
server = "Deepak-Laptop"
database = "MemberPortal"

def tc1():
    # Test Case 1
    validator.start_test_case ("TC1")
    result = validator.validate_csv_record_count(csv_file_path, "> 20000")    
    validator.end_test_case ("TC1", result, "")

def tc2():
    # Test Case 2
    validator.start_test_case ("TC2")
    csv_sql = "select Model, count(*) count from myCSVFile Group By Model order by count desc limit 10"
    database_sql = "select top 10 Model, count(*) count from Electric_Vehicle_Population_Data Group By Model order by count desc"

    csv_df = validator.get_records_csv(csv_file_path, "myCSVFile", csv_sql)
    sql_df = validator.get_records_sql_server(server, database, database_sql)

    validator.compare_csv_with_db(csv_df, sql_df, key_columns=['Model'])

    #result = True if len(df) == 10 else False
    validator.end_test_case ("TC2", True, "")

def tc2():
    validator.start_test_case ("TC2")

    # sql statements
    csv_sql = "select Model, count(*) count from myCSVFile Group By Model order by count desc limit 10"
    database_sql = "select top 10 Model, count(*) count from Electric_Vehicle_Population_Data Group By Model order by count desc"

    # get data
    csv_df = validator.get_records_csv(csv_file_path, "myCSVFile", csv_sql)
    sql_df = validator.get_records_sql_server(server, database, database_sql)

    # compare data
    result = validator.compare_csv_with_db(csv_df, sql_df, key_columns=['Model'])

    validator.end_test_case ("TC2", result, "")


def tc2():
    validator.start_test_case ("TC2")

    # sql statements
    csv_sql = "select Model, count(*) count from myCSVFile Group By Model order by count desc limit 10"
    database_sql = "select top 10 Model, count(*) count from Electric_Vehicle_Population_Data Group By Model order by count desc"

    # get data
    csv_df = validator.get_records_csv(csv_file_path, "myCSVFile", csv_sql)
    sql_df = validator.get_records_sql_server(server, database, database_sql)

    # compare data
    result = validator.compare_csv_with_db(csv_df, sql_df, key_columns=['Model'])

    validator.end_test_case ("TC2", result, "")

def tc3():
    validator.start_test_case ("TC2")

    # sql statements
    #csv_sql = "select \"VIN (1-10)\" VIN from myCSVFile limit 10"
    csv_sql = "select \"VIN (1-10)\" VIN ,County,City,State,Postal Code,Model Year,Make,Model from myCSVFile limit 10"
    database_sql = "select top 10 Model, count(*) count from Electric_Vehicle_Population_Data Group By Model order by count desc"

    # get records
    csv_df = validator.get_records_csv(csv_file_path, "myCSVFile", csv_sql)

    print (csv_df)

    # get VINs
    vin_list = validator.convert_df_string (csv_df)

    print(vin_list)

    # get data
    #csv_df = validator.get_records_csv(csv_file_path, "myCSVFile", csv_sql)
    #sql_df = validator.get_records_sql_server(server, database, database_sql)

    # compare data
    #result = validator.compare_csv_with_db(csv_df, sql_df, key_columns=['Model'])

    #validator.end_test_case ("TC2", result, "")

print ("Test Case  | Status | Time Taken | Message")
print ("------------------------------------------")

#tc1()
#tc2()
tc3()

print ("------------------------------------------")
