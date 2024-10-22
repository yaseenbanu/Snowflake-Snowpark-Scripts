import os
from dotenv import load_dotenv
import snowflake.connector
import snowflake.snowpark as snowpark

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials from environment variables
user = os.getenv('SNOWFLAKE_USER')
password = os.getenv('SNOWFLAKE_PASSWORD')
account = os.getenv('SNOWFLAKE_ACCOUNT')
role = os.getenv('SNOWFLAKE_ROLE')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
database = os.getenv('SNOWFLAKE_DATABASE')  # Add this to your .env if needed
schema = os.getenv('SNOWFLAKE_SCHEMA')      # Add this to your .env if needed

# Establish connection to Snowflake
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,  # Your account name
    role=role,        # Your role
    warehouse=warehouse,  # Your warehouse
    database=database,    # Your database
    schema=schema         # Your schema
)

# Create a Snowpark session with the connection parameters
session = snowpark.Session.builder.configs({
    'user': user,
    'password': password,
    'account': account,
    'role': role,
    'warehouse': warehouse,
    'database': database,
    'schema': schema
}).create()

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    tableName_1 = 'TPCH_SF1.CUSTOMER'
    tableName_2 = 'TPCH_SF1.ORDERS'
    
    df_1 = session.table(tableName_1)
    df_2 = session.table(tableName_2)
    
    # Perform the join on C_CUSTKEY from CUSTOMER and O_CUSTKEY from ORDERS
    joined_df = df_1.join(df_2, df_1['C_CUSTKEY'] == df_2['O_CUSTKEY'])
    
    # Show a sample of the joined dataframe
    joined_df.show()

    # Return the joined dataframe, which will appear in the Results tab
    return joined_df

# Call the main function
if __name__ == "__main__":
    main(session)

# Close the connection to Snowflake (optional, as it will be closed at the end of the program)
conn.close()
