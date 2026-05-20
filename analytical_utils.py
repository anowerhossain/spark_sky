from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, rank, dense_rank,
    sum as _sum,
    avg as _avg,
    lag, lead, col
)


def add_rank(
    df: DataFrame,
    partition_cols: list,
    order_col: str,
    order: str = "desc"
):
    """
    Rank with dynamic ordering (ASC / DESC)
    """

    if order.lower() == "asc":
        window = Window.partitionBy(*partition_cols).orderBy(col(order_col).asc())
    else:
        window = Window.partitionBy(*partition_cols).orderBy(col(order_col).desc())

    return df.withColumn("rank", rank().over(window))


def add_dense_rank(
    df: DataFrame,
    partition_cols: list,
    order_col: str,
    order: str = "desc"   # default behavior
):
    """
    Dense rank with dynamic ordering (ASC / DESC)
    """

    if order.lower() == "asc":
        window = Window.partitionBy(*partition_cols).orderBy(col(order_col).asc())
    else:
        window = Window.partitionBy(*partition_cols).orderBy(col(order_col).desc())

    return df.withColumn("dense_rank", dense_rank().over(window))


def running_total(
    df: DataFrame,
    partition_cols: list,
    order_col: str,
    value_col: str,
    order: str = "asc"
):
    """
    Running total with ASC/DESC support
    """

    if order.lower() == "desc":
        order_expr = col(order_col).desc()
    else:
        order_expr = col(order_col).asc()

    window = Window.partitionBy(*partition_cols).orderBy(order_expr)

    return df.withColumn(
        "running_total",
        _sum(col(value_col)).over(window)
    )



def running_total_and_percentage(
    df: DataFrame,
    partition_cols: list,
    order_col: str,
    value_col: str,
    order: str = "asc",
    running_total_col: str = "running_total",
    running_percentage_col: str = "running_percentage"
):
    """
    Computes:
    1. Running Total
    2. Running Percentage

    Supports ASC / DESC ordering

    Common DWH use cases:
    - revenue contribution %
    - cumulative sales
    - risk exposure tracking
    """

    # -------------------------------------------------
    # 1. Define ordering direction
    # -------------------------------------------------
    if order.lower() == "desc":
        order_expr = col(order_col).desc()
    else:
        order_expr = col(order_col).asc()

    # -------------------------------------------------
    # 2. Running window (ordered)
    # -------------------------------------------------
    running_window = Window.partitionBy(*partition_cols).orderBy(order_expr)

    # -------------------------------------------------
    # 3. Total window (no ordering)
    # -------------------------------------------------
    total_window = Window.partitionBy(*partition_cols)

    # -------------------------------------------------
    # 4. Compute running total
    # -------------------------------------------------
    df = df.withColumn(
        running_total_col,
        _sum(col(value_col)).over(running_window)
    )

    # -------------------------------------------------
    # 5. Compute total sum
    # -------------------------------------------------
    df = df.withColumn(
        "total_sum",
        _sum(col(value_col)).over(total_window)
    )

    # -------------------------------------------------
    # 6. Compute running percentage
    # -------------------------------------------------
    df = df.withColumn(
        running_percentage_col,
        (col(running_total_col) / col("total_sum")) * 100
    )

    return df


def add_lag(df: DataFrame, partition_cols: list, order_col: str, value_col: str):
    window = Window.partitionBy(*partition_cols).orderBy(col(order_col))

    return df.withColumn("prev_value", lag(col(value_col)).over(window))


def add_lead(df: DataFrame, partition_cols: list, order_col: str, value_col: str):
    window = Window.partitionBy(*partition_cols).orderBy(col(order_col))

    return df.withColumn("next_value", lead(col(value_col)).over(window))

