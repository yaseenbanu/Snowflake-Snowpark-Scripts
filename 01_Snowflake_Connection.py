import os
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials from environment variables
user = os.getenv('SNOWFLAKE_USER')
password = os.getenv('SNOWFLAKE_PASSWORD')
account = os.getenv('SNOWFLAKE_ACCOUNT')
role = os.getenv('SNOWFLAKE_ROLE')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
database = os.getenv('SNOWFLAKE_DATABASE') 
schema = os.getenv('SNOWFLAKE_SCHEMA')  

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

# Create a cursor object
cur = conn.cursor()

# Retrieve session details
cur.execute("SELECT 1")

# Fetch and print the results
result = cur.fetchone()

print(f"Result: {result}")

# Close the cursor and connection
cur.close()
conn.close()
