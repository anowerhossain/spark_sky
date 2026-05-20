from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    lit,
    current_timestamp,
    current_date,
    row_number,
    rank,
    dense_rank,
    monotonically_increasing_id,
    sha2,
    concat_ws,
    col,
    when,
    date_format, 
    to_date,
    hour,
    quarter,
    dayofweek,
    to_timestamp
)
from pyspark.sql.window import Window



# =========================================================
# 1. EXTRACT HOUR
# =========================================================
def add_hour(df: DataFrame, column_name: str, new_column: str = "hour"):
    """
    Extracts hour from timestamp

    Example:
    2026-05-20 14:35:10 → 14
    """

    return df.withColumn(
        new_column,
        hour(col(column_name))
    )



# =========================================================
# 2. EXTRACT QUARTER
# =========================================================
def add_quarter(df: DataFrame, column_name: str, new_column: str = "quarter"):
    """
    Extracts quarter from date

    Example:
    2026-05-20 → 2
    """

    return df.withColumn(
        new_column,
        quarter(col(column_name))
    )


# =========================================================
# 3. EXTRACT WEEKDAY NUMBER
# =========================================================
def add_weekday(df: DataFrame, column_name: str, new_column: str = "weekday"):
    """
    Returns weekday number

    Spark:
    1 = Sunday
    2 = Monday
    ...
    7 = Saturday
    """

    return df.withColumn(
        new_column,
        dayofweek(col(column_name))
    )


# =========================================================
# 4. EXTRACT WEEKDAY NAME
# =========================================================
def add_weekday_name(df: DataFrame, column_name: str, new_column: str = "weekday_name"):
    """
    Returns weekday name

    Example:
    Monday, Tuesday
    """

    return df.withColumn(
        new_column,
        date_format(col(column_name), "EEE")
    )



def add_year_month_key(df: DataFrame, column_name: str, new_column: str = "year_month"):
    """
    Returns YYYY-MM format (string)
    """

    return df.withColumn(
        new_column,
        date_format(to_date(col(column_name)), "yyyy-MM")
    )


def add_date_key(df: DataFrame, column_name: str, new_column: str = "date_key"):
    """
    Converts date → integer key format: YYYYMMDD

    Example:
    2026-05-20 → 20260520
    """

    return df.withColumn(
        new_column,
        date_format(to_date(col(column_name)), "yyyyMMdd").cast("int")
    )


def add_month_key(df: DataFrame, column_name: str, new_column: str = "month_key"):
    """
    Converts date → YYYYMM format

    Example:
    2026-05-20 → 202605
    """

    return df.withColumn(
        new_column,
        date_format(to_date(col(column_name)), "yyyyMM").cast("int")
    )



def add_year_key(df: DataFrame, column_name: str, new_column: str = "year"):
    return df.withColumn(
        new_column,
        date_format(to_date(col(column_name)), "yyyy").cast("int")
    )




def add_constant_column(df: DataFrame, column_name: str, value):
    """
    Adds a fixed constant column
    Example: source = 'api'
    """
    return df.withColumn(column_name, lit(value))


def add_current_timestamp(df: DataFrame, column_name: str = "ingestion_time"):
    return df.withColumn(column_name, current_timestamp())


def add_current_date(df: DataFrame, column_name: str = "ingestion_date"):
    return df.withColumn(column_name, current_date())


def add_surrogate_key(df: DataFrame, column_name: str = "surrogate_id"):
    """
    Distributed unique ID (NOT sequential)
    """
    return df.withColumn(column_name, monotonically_increasing_id())



def add_hash_column(df: DataFrame, cols: list, column_name: str = "row_hash"):
    """
    Creates a hash for change detection (SCD, CDC)
    """
    return df.withColumn(column_name, sha2(concat_ws("||", *[col(c) for c in cols]), 256))



def add_flag(df: DataFrame, column_name: str, condition):
    """
    Adds flag column based on condition expression
    """
    return df.withColumn(column_name, condition)
  

def add_incremental_id(df: DataFrame, column_name: str = "increment_id"):
    return df.withColumn(column_name, monotonically_increasing_id())


def add_case_when_column(
    df: DataFrame,
    column_name: str,
    conditions: list,
    default_value=None
):
    """
    Safe CASE WHEN builder (supports single + multiple conditions)

    conditions format:
    [
        (condition1, value1),
        (condition2, value2),
        ...
    ]
    """

    if not conditions:
        return df

    expr = when(conditions[0][0], conditions[0][1])

    for cond, value in conditions[1:]:
        expr = expr.when(cond, value)

    if default_value is not None:
        expr = expr.otherwise(default_value)

    return df.withColumn(column_name, expr)




def mask_column(df: DataFrame, column_name: str, visible_chars: int = 2):
    return df.withColumn(
        column_name,
        concat(
            substring(col(column_name), 1, visible_chars),
            lit("*****")
        )
    )



def remove_prefix(df: DataFrame, column_name: str, prefix: str):
    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), f"^{prefix}", "")
    )


def remove_suffix(df: DataFrame, column_name: str, suffix: str):
    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), f"{suffix}$", "")
    )


def keep_numeric_only(df: DataFrame, column_name: str):
    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), "[^0-9]", "")
    )


def keep_alpha_only(df: DataFrame, column_name: str):
    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), "[^a-zA-Z]", "")
    )

