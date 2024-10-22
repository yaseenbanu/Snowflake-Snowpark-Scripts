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
    # Use the appropriate schema
    session.sql("USE SCHEMA snowflake_sample_data.tpch_sf1").collect()

    # Execute the Pricing Summary Report Query (Q1)
    query = """
    SELECT
        l_returnflag,
        l_linestatus,
        SUM(l_quantity) AS sum_qty,
        SUM(l_extendedprice) AS sum_base_price,
        SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        AVG(l_quantity) AS avg_qty,
        AVG(l_extendedprice) AS avg_price,
        AVG(l_discount) AS avg_disc,
        COUNT(*) AS count_order
    FROM lineitem
    WHERE l_shipdate <= DATEADD(day, -90, TO_DATE('1998-12-01'))
    GROUP BY l_returnflag, l_linestatus
    ORDER BY l_returnflag, l_linestatus;
    """

    # Execute the query and get the results
    results_df = session.sql(query).collect()

    # Print the results
    for row in results_df:
        print(row)

# Call the main function
if __name__ == "__main__":
    main(session)

conn.close()
