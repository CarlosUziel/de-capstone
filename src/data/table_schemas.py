from typing import Dict

import pyspark.sql.types as T

# I94 Immigration Data
I94_ON_LOAD_FIELDS: Dict[str, T.DataType] = {
    "cicid": (T.FloatType(), True),
    "i94yr": (T.FloatType(), True),
    "i94mon": (T.FloatType(), True),
    "i94cit": (T.FloatType(), True),
    "i94res": (T.FloatType(), True),
    "i94port": (T.StringType(), True),
    "arrdate": (T.FloatType(), True),
    "i94mode": (T.FloatType(), True),
    "i94addr": (T.StringType(), True),
    "depdate": (T.FloatType(), True),
    "i94bir": (T.FloatType(), True),
    "i94visa": (T.FloatType(), True),
    "count": (T.FloatType(), True),
    "dtadfile": (T.FloatType(), True),
    "visapost": (T.StringType(), True),
    "occup": (T.StringType(), True),
    "entdepa": (T.StringType(), True),
    "entdepd": (T.StringType(), True),
    "entdepu": (T.StringType(), True),
    "matflag": (T.StringType(), True),
    "biryear": (T.FloatType(), True),
    "dtaddto": (T.FloatType(), True),
    "gender": (T.StringType(), True),
    "insnum": (T.FloatType(), True),
    "airline": (T.StringType(), True),
    "admnum": (T.FloatType(), True),
    "fltno": (T.StringType(), True),
    "visatype": (T.StringType(), True),
}
I94_ON_LOAD_SCHEMA = T.StructType(
    [T.StructField(k, v[0], v[1]) for k, v in I94_ON_LOAD_FIELDS.items()]
)

# U.S. City Demographic Data
USDEMO_ON_LOAD_FIELDS: Dict[str, T.DataType] = {
    "City": (T.StringType(), False),
    "State": (T.StringType(), True),
    "Median Age": (T.FloatType(), True),
    "Male Population": (T.FloatType(), True),
    "Female Population": (T.FloatType(), True),
    "Total Population": (T.IntegerType(), True),
    "Number of Veterans": (T.FloatType(), True),
    "Foreign-born": (T.FloatType(), True),
    "Average Household Size": (T.FloatType(), True),
    "State Code": (T.StringType(), True),
    "Race": (T.StringType(), True),
    "Count": (T.IntegerType(), True),
}
USDEMO_ON_LOAD_SCHEMA = T.StructType(
    [T.StructField(k, v[0], v[1]) for k, v in USDEMO_ON_LOAD_FIELDS.items()]
)

# Airport Code Table
AIRCODES_ON_LOAD_FIELDS: Dict[str, T.DataType] = {
    "ident": (T.StringType(), False),
    "type": (T.StringType(), True),
    "name": (T.StringType(), True),
    "elevation_ft": (T.FloatType(), True),
    "continent": (T.StringType(), True),
    "iso_country": (T.StringType(), True),
    "iso_region": (T.StringType(), True),
    "municipality": (T.StringType(), True),
    "gps_code": (T.StringType(), True),
    "iata_code": (T.StringType(), True),
    "local_code": (T.StringType(), True),
    "coordinates": (T.StringType(), True),
}
AIRCODES_ON_LOAD_SCHEMA = T.StructType(
    [T.StructField(k, v[0], v[1]) for k, v in AIRCODES_ON_LOAD_FIELDS.items()]
)

# World Temperature Data
TEMP_ON_LOAD_FIELDS: Dict[str, T.DataType] = {
    "dt": (T.StringType(), False),
    "AverageTemperature": (T.FloatType(), True),
    "AverageTemperatureUncertainty": (T.FloatType(), True),
    "City": (T.StringType(), True),
    "Country": (T.StringType(), True),
    "Latitude": (T.StringType(), True),
    "Longitude": (T.StringType(), True),
}
TEMP_ON_LOAD_SCHEMA = T.StructType(
    [T.StructField(k, v[0], v[1]) for k, v in TEMP_ON_LOAD_FIELDS.items()]
)

# Dictionary of staging tables' schemas
ON_LOAD_TABLES_SCHEMA: Dict[str, T.StructType] = {
    "i94_immigration": I94_ON_LOAD_SCHEMA,
    "us_demographics": USDEMO_ON_LOAD_SCHEMA,
    "airport_codes": AIRCODES_ON_LOAD_SCHEMA,
    "world_temperature": TEMP_ON_LOAD_SCHEMA,
}
