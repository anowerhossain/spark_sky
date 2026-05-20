from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper,
    initcap,
    regexp_replace,
    substring,
    length,
    concat,
    lit,
    split
)

# =========================================================
# 1. TRIM (MODIFIES EXISTING COLUMN)
# =========================================================
def trim_column(df: DataFrame, column_name: str):
    """
    Removes leading and trailing spaces
    """
    return df.withColumn(column_name, trim(col(column_name)))


# Example:
# df = trim_column(df, "name")


# =========================================================
# 2. LOWERCASE (MODIFIES EXISTING COLUMN)
# =========================================================
def to_lower(df: DataFrame, column_name: str):
    return df.withColumn(column_name, lower(col(column_name)))


# Example:
# df = to_lower(df, "email")


# =========================================================
# 3. UPPERCASE (MODIFIES EXISTING COLUMN)
# =========================================================
def to_upper(df: DataFrame, column_name: str):
    return df.withColumn(column_name, upper(col(column_name)))


# Example:
# df = to_upper(df, "country")


# =========================================================
# 4. TITLE CASE (CREATES NEW COLUMN - optional transformation)
# =========================================================
def to_title_case(df: DataFrame, column_name: str, new_column: str):
    return df.withColumn(new_column, initcap(col(column_name)))


# Example:
# df = to_title_case(df, "name", "name_title")


# =========================================================
# 5. REMOVE SPECIAL CHARACTERS
# =========================================================
def remove_special_chars(df: DataFrame, column_name: str, pattern: str = "[^a-zA-Z0-9 ]"):
    return df.withColumn(column_name, regexp_replace(col(column_name), pattern, ""))


# Example:
# df = remove_special_chars(df, "customer_name")


# =========================================================
# 6. NORMALIZE SPACES (MULTIPLE SPACES → SINGLE SPACE)
# =========================================================
def normalize_spaces(df: DataFrame, column_name: str):
    return df.withColumn(column_name, regexp_replace(col(column_name), "\\s+", " "))


# Example:
# df = normalize_spaces(df, "address")


# =========================================================
# 7. SUBSTRING EXTRACTION
# =========================================================
def substring_column(df: DataFrame, column_name: str, start: int, length_val: int, new_column: str):
    return df.withColumn(new_column, substring(col(column_name), start, length_val))


# Example:
# df = substring_column(df, "account_id", 1, 4, "prefix")


# =========================================================
# 8. STRING LENGTH (DATA QUALITY CHECK)
# =========================================================
def add_string_length(df: DataFrame, column_name: str, new_column: str = "length"):
    return df.withColumn(new_column, length(col(column_name)))


# Example:
# df = add_string_length(df, "name")


# =========================================================
# 9. CONCAT COLUMNS
# =========================================================
def concat_columns(df: DataFrame, columns: list, new_column: str, separator: str = " "):
    return df.withColumn(new_column, concat(*[col(c) for c in columns]))


# Example:
# df = concat_columns(df, ["first_name", "last_name"], "full_name")


# =========================================================
# 10. MASKING (PII SECURITY)
# =========================================================
def mask_column(df: DataFrame, column_name: str, visible_chars: int = 2):
    return df.withColumn(
        column_name,
        concat(
            substring(col(column_name), 1, visible_chars),
            lit("*****")
        )
    )


# Example:
# df = mask_column(df, "phone")


# =========================================================
# 11. SPLIT COLUMN INTO MULTIPLE COLUMNS
# =========================================================
def split_column(df: DataFrame, column_name: str, delimiter: str, new_columns: list):
    split_col = split(col(column_name), delimiter)

    for i, new_col in enumerate(new_columns):
        df = df.withColumn(new_col, split_col.getItem(i))

    return df


# Example:
# df = split_column(df, "full_name", " ", ["first_name", "last_name"])



def pad_left(df: DataFrame, column_name: str, length_val: int, pad_char: str = "0"):
    return df.withColumn(
        column_name,
        lpad(col(column_name), length_val, pad_char)
    )




def pad_right(df: DataFrame, column_name: str, length_val: int, pad_char: str = " "):
    return df.withColumn(
        column_name,
        rpad(col(column_name), length_val, pad_char)
    )
