#!/usr/bin/env python
# coding: utf-8

# ## DQ_run_tests
# 
# 
# 

# ## Imports and Constants

# In[4]:


get_ipython().run_line_magic('run', '"DQ_funcs"')


# In[5]:


DQ_DB = "data_quality_tests"
DQ_TEST_DEFINITION_TABLE = "dq_tests"
DQ_TEST_OUTPUT_TABLE = "test_outputs"


# ## Run the tests and write output to the database

# In[6]:


# Run setup which will test creating database if required
setUpDB(DQ_DB, DQ_TEST_DEFINITION_TABLE, DQ_TEST_OUTPUT_TABLE)

dq_tests = [
    ("XXXX", "XXXX_application", "FEE_DUE", "Completeness", "Single-Field", "FEE_DUE IS NOT NULL", "FEE_DUE IS NOT NULL"),
    ("XXXX", "XXXX_application", "FEE_DUE", "Precision", "Numerical", "Does every value have two (and only two) decimal places?", "CAST(FEE_DUE as varchar(10)) <> 2 OR FEE_DUE IS NULL"),
    ("XXXX", "XXXX_application", "MANUFACTURER_ID", "Completeness", "Single-Field", "MANUFACTURER_ID IS NOT NULL", "MANUFACTURER_ID IS NULL"),
    ("XXXX", "XXXX_site", "INVOICE_SITE_PARTY_ID", "Completeness", "Single-Field", "INVOICE_SITE_PARTY_ID IS NOT NULL", "INVOICE_SITE_PARTY_ID IS NOT NULL"), 
    ("XXXX", "XXXX_site", "CASE_ID", "Completeness", "Single-Field", "CASE_ID IS NOT NULL", "CASE_ID IS NULL"),
    ("XXXX_cleaned", "products", "PRODUCT_ID", "Completeness", "Single-Field", "PRODUCT_ID IS NOT NULL", "PRODUCT_ID IS NOT NULL"),       
    ("XXXX_cleaned", "products", "PRODUCT_NAME", "Completeness", "Single-Field", "PRODUCT_NAME IS NOT NULL", "PRODUCT_NAME IS NOT NULL"),               ]

addTestsToDB(dq_tests, DQ_DB, DQ_TEST_DEFINITION_TABLE)

runDefinedTests(DQ_DB, DQ_TEST_DEFINITION_TABLE, DQ_TEST_OUTPUT_TABLE)


# ## Run tests across all fields in all tables in databases

# In[67]:


db = ["XXXX_cleaned"]

for d in db:
#    tbl = spark.sql(f"SHOW TABLES IN {d}").collect()
tbl = ["additional_case_reasons", "case_collections", "case_orders", "case_step_details", "case_types", "case_workflow_steps"]
    for t in tbl:
        columns = spark.sql(f"SHOW COLUMNS IN {d}.{t}").collect()
        for col in columns:
            completenessTest(db, t, col.col_name, DQ_DB + "." + DQ_TEST_OUTPUT_TABLE)


# 
