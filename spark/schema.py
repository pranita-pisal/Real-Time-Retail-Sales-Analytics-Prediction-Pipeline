from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType

# Define the exact schema for the Walmart dataset incoming from Kafka
walmart_schema = StructType([
    StructField("Store", IntegerType(), True),
    StructField("Dept", IntegerType(), True),
    StructField("Date", StringType(), True),
    StructField("Weekly_Sales", DoubleType(), True),
    StructField("IsHoliday", BooleanType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Fuel_Price", DoubleType(), True),
    StructField("CPI", DoubleType(), True),
    StructField("Unemployment", DoubleType(), True)
])

def get_schema():
    """
    Returns the PySpark StructType schema for the Walmart dataset.
    This is used by the structured streaming job to parse the JSON payloads.
    """
    return walmart_schema
