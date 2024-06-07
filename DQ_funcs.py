#!/usr/bin/env python
# coding: utf-8

# ## DQ_funcs
# 
# 
# 

# ## Imports

# In[ ]:


from pyspark.sql.types import *


# ## Setup functions

# In[5]:


def createTestDefinitionTable(db, tbl):
    if spark.catalog.tableExists(tbl, db):
        print(f"Table {tbl} already exists, exiting function createTestDefinitionTable()")
    else:
        spark.sql("DROP TABLE IF EXISTS " + db + '.' + tbl)

        emp_RDD = spark.sparkContext.emptyRDD()

        columns = StructType([StructField('source_system', StringType(), True),
                            StructField('object', StringType(), True),
                            StructField('column', StringType(), True),            
                            StructField('dq_dimension', StringType(), True),
                            StructField('dq_subdimension', StringType(), True),
                            StructField('test_description', StringType(), True),                                
                            StructField('test', StringType(), True)])
    
        df = spark.createDataFrame(data=emp_RDD, schema=columns)
        df.write.mode("overwrite").format("delta").saveAsTable(db + "." + tbl)

def createTestOutputTable(db, tbl):
    if spark.catalog.tableExists(tbl, db):
        print(f"Table {tbl} already exists, exiting function createTestoutputTable()")
    else:
        spark.sql(f"DROP TABLE IF EXISTS {db}.{tbl}")

        emp_RDD = spark.sparkContext.emptyRDD()
        
        columns = StructType([StructField('rows_passed', LongType(), True),
                            StructField('total_num_rows', LongType(), True),
                            StructField('test_time', TimestampType(), True),
                            StructField('source', StringType(), True),
                            StructField('table', StringType(), True),            
                            StructField('field', StringType(), True),
                            StructField('dq_dimension', StringType(), True),
                            StructField('dq_subdimension', StringType(), True),                                
                            StructField('test_description', StringType(), True),
                            StructField('test', StringType(), True)])

        df = spark.createDataFrame(data = emp_RDD, schema = columns)
        df.write.mode("overwrite").format("delta").saveAsTable(db + '.' + tbl)

def addTestsToDB(tests, db, tbl):
    dq_columns = ["source_system", "object", "column", "dq_dimension", "dq_subdimension", "test_description", "test"]
    df_dq = spark.createDataFrame(data=tests, schema=dq_columns)
    df_dq.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(db + "." + tbl)
    # Need to update this such that we add unique rows that have been added by doing the equivalent of an upsert, but an overwrite is fine for the moment

def setUpDB(db, test_definition_tbl, test_output_tbl):
    spark.sql("CREATE DATABASE IF NOT EXISTS " + db)
    createTestOutputTable(db, test_output_tbl)
    createTestDefinitionTable(db, test_definition_tbl)

def deleteDQDatabase(db):
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCASE")


# # Tests

# In[6]:


def runDefinedTests(db, test_tbl, out_tbl): 
    if not spark.catalog.tableExists(test_tbl, db):
        mssparkutils.notebook.exit(f"Error!! Exit runDefinedTests, table {test_tbl} does not exist!")
 
    if not spark.catalog.tableExists(out_tbl, db):
        mssparkutils.notebook.exit(f"Error!! Exit runDefinedTests, table {out_tbl} does not exist!")

    dq_tests = spark.sql(f"SELECT * FROM {db}.{test_tbl}").collect()

    for test in dq_tests:
        test_sql = f"""
            SELECT
                a.result_passing rows_passed,
                (SELECT COUNT(*) FROM {test[0]}.{test[1]}) total_num_rows,
                current_timestamp() test_time,
                \"{test[0]}\" source,
                \"{test[1]}\" table,
                \"{test[2]}\" field,
                \"{test[3]}\" dq_dimension,
                \"{test[4]}\" dq_subdimension,
                \"{test[5]}\" test_description,
                \"{test[6]}\" test
            FROM
            (
                SELECT 
                    COUNT(*) result_passing
                FROM {test[0]}.{test[1]}
                WHERE {test[6]} 
            ) a
                """
        # run the DQ test and write to table
        df_test_output = spark.sql(test_sql)

        df_test_output.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(db + "." + out_tbl)

def completenessTest(test_db, test_tbl, test_field, dq_db, out_tbl):

    if not spark.catalog.tableExists(out_tbl, dq_db):
        mssparkutils.notebook.exit(f"Error!! Exit completenessTest, table {out_tbl} does not exist!")

    test_sql = f"""
    SELECT
        a.result_passing rows_passed,
        (SELECT COUNT(*) FROM {test_db}.{test_tbl}) total_num_rows,
        current_timestamp() test_time,
        \"{test_db}\" source,
        \"{test_tbl}\" table,
        \"{test_field}\" field,
        \"Completeness\" dq_dimension,
        \"Single-field" dq_subdimension,
        \"Field IS NOT NULL\" test_description,
        \"IS NOT NULL\" test
    FROM
    (
        SELECT 
            COUNT(*) result_passing
        FROM {test_db}.{test_tbl}
        WHERE {test_field} IS NOT NULL 
    ) a
        """

    df_test_output = spark.sql(test_sql)
    df_test_output.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(dq_db + "." + out_tbl)
        """
