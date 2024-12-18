import os
from dotenv import load_dotenv
import snowflake.snowpark as snowpark

# Load environment variables
load_dotenv()

# Retrieve credentials from environment variables
user = os.getenv('SNOWFLAKE_USER')
password = os.getenv('SNOWFLAKE_PASSWORD')
account = os.getenv('SNOWFLAKE_ACCOUNT')
role = os.getenv('SNOWFLAKE_ROLE')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')

database_input = os.getenv('SNOWFLAKE_DATABASE_INPUT')  # e.g., SNOWFLAKE_SAMPLE_DATA
schema_input = os.getenv('SNOWFLAKE_SCHEMA_INPUT')      # e.g., TPCH_SF1
database_output = os.getenv('SNOWFLAKE_DATABASE_OUTPUT') # Your private database
schema_output = os.getenv('SNOWFLAKE_SCHEMA_OUTPUT')     # e.g., PUBLIC

# Create sessions
session_1 = snowpark.Session.builder.configs({
    'user': user,
    'password': password,
    'account': account,
    'role': role,
    'warehouse': warehouse,
    'database': database_output,
    'schema': schema_output
}).create()

session_2 = snowpark.Session.builder.configs({
    'user': user,
    'password': password,
    'account': account,
    'role': role,
    'warehouse': warehouse,
    'database': database_input,
    'schema': schema_input
}).create()

# Load table from the shared database using session_2
df_1 = session_2.table(f"{database_input}.{schema_input}.CUSTOMER")

# Optionally, transform the data (this example simply selects)
transformed_df = df_1.select("*")  # Modify as needed

# Show the transformed DataFrame
transformed_df.show()

# Write to the destination table using session_1
transformed_df.write.mode("overwrite").save_as_table(f"{database_output}.{schema_output}.CUSTOMER_TRANSFORMED")
