from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# -----------------------------
# INTERNAL: Align schemas
# -----------------------------
def _align_columns(df1: DataFrame, df2: DataFrame):
    """
    Ensure both DataFrames have identical columns (adds missing columns as null)
    """
    all_cols = list(set(df1.columns).union(set(df2.columns)))

    def normalize(df: DataFrame):
        for c in all_cols:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None))
        return df.select(all_cols)

    return normalize(df1), normalize(df2)


# -----------------------------
# UNION ALL (Spark unionByName)
# -----------------------------
def union_all(df1: DataFrame, df2: DataFrame) -> DataFrame:
    df1, df2 = _align_columns(df1, df2)
    return df1.unionByName(df2)


# -----------------------------
# UNION DISTINCT
# -----------------------------
def union(df1: DataFrame, df2: DataFrame) -> DataFrame:
    return union_df(df1, df2).distinct()


# -----------------------------
# EXCEPT (df1 - df2)
# -----------------------------
def except_df(df1: DataFrame, df2: DataFrame) -> DataFrame:
    df1, df2 = _align_columns(df1, df2)
    return df1.exceptAll(df2)
