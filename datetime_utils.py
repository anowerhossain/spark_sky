
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    date_format,
    unix_timestamp,
    from_unixtime,
    current_timestamp,
    current_date,
    year,
    month,
    dayofmonth,
    hour,
    datediff
)
from datetime import datetime


# -------------------------------------------------
# 1. STRING → TIMESTAMP / DATE PARSING
# -------------------------------------------------

def parse_timestamp(df: DataFrame, column: str, format: str = "yyyy-MM-dd HH:mm:ss"):
    return df.withColumn(column, to_timestamp(col(column), format))


def parse_date(df: DataFrame, column: str, format: str = "yyyy-MM-dd"):
    return df.withColumn(column, to_date(col(column), format))


# -------------------------------------------------
# 2. FORMAT STANDARDIZATION
# -------------------------------------------------

def format_timestamp(df: DataFrame, column: str, output_format: str = "yyyy-MM-dd HH:mm:ss"):
    return df.withColumn(column, date_format(col(column), output_format))


# -------------------------------------------------
# 3. UNIX TIME CONVERSIONS
# -------------------------------------------------

def to_unix_timestamp(df: DataFrame, column: str):
    return df.withColumn(column, unix_timestamp(col(column)))


def from_unix_timestamp(df: DataFrame, column: str, format: str = "yyyy-MM-dd HH:mm:ss"):
    return df.withColumn(column, from_unixtime(col(column), format))


# -------------------------------------------------
# 4. CURRENT DATE / TIMESTAMP
# -------------------------------------------------

def add_current_timestamp(df: DataFrame, column_name: str = "ingestion_time"):
    return df.withColumn(column_name, current_timestamp())


def add_current_date(df: DataFrame, column_name: str = "ingestion_date"):
    return df.withColumn(column_name, current_date())


# -------------------------------------------------
# 5. TIME FEATURE EXTRACTION (DWH DIMENSIONS)
# -------------------------------------------------

def extract_time_features(df: DataFrame, column: str):
    return (
        df.withColumn("year", year(col(column)))
          .withColumn("month", month(col(column)))
          .withColumn("day", dayofmonth(col(column)))
          .withColumn("hour", hour(col(column)))
    )


# -------------------------------------------------
# 6. DATE DIFFERENCE (SLA / DELAY ANALYSIS)
# -------------------------------------------------

def add_date_difference(df: DataFrame, start_col: str, end_col: str, output_col: str = "diff_days"):
    return df.withColumn(output_col, datediff(col(end_col), col(start_col)))


# -------------------------------------------------
# 7. BUSINESS DATE ALIGNMENT
# -------------------------------------------------

def align_business_date(df: DataFrame, column: str):
    return df.withColumn(column, to_date(col(column)))


# -------------------------------------------------
# 8. SAFE TIMESTAMP CASTING
# -------------------------------------------------

def safe_timestamp_cast(df: DataFrame, column: str):
    return df.withColumn(column, to_timestamp(col(column)))


# -------------------------------------------------
# 9. PYTHON DATETIME UTILITIES (NON-SPARK)
# -------------------------------------------------

def format_python_datetime(dt: datetime, format: str = "%Y-%m-%d %H:%M:%S"):
    return dt.strftime(format)


def parse_python_datetime(date_str: str, format: str = "%Y-%m-%d %H:%M:%S"):
    return datetime.strptime(date_str, format)
