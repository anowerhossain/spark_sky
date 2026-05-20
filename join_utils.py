from pyspark.sql.functions import broadcast
from pyspark.sql import DataFrame


def inner_join(
    left_df: DataFrame,
    right_df: DataFrame,
    join_condition,
    broadcast_right=False
):

    if broadcast_right:
        right_df = broadcast(right_df)

    return left_df.join(
        right_df,
        join_condition,
        "inner"
    )


def left_join(
    left_df: DataFrame,
    right_df: DataFrame,
    join_condition,
    broadcast_right=False
):

    if broadcast_right:
        right_df = broadcast(right_df)

    return left_df.join(
        right_df,
        join_condition,
        "left"
    )


def right_join(
    left_df: DataFrame,
    right_df: DataFrame,
    join_condition,
    broadcast_right=False
):

    if broadcast_right:
        right_df = broadcast(right_df)

    return left_df.join(
        right_df,
        join_condition,
        "right"
    )


def full_join(
    left_df: DataFrame,
    right_df: DataFrame,
    join_condition,
    broadcast_right=False
):

    if broadcast_right:
        right_df = broadcast(right_df)

    return left_df.join(
        right_df,
        join_condition,
        "full"
    )


def left_anti_join(
    left_df: DataFrame,
    right_df: DataFrame,
    join_condition
):

    return left_df.join(
        right_df,
        join_condition,
        "left_anti"
    )


def left_semi_join(
    left_df: DataFrame,
    right_df: DataFrame,
    join_condition
):

    return left_df.join(
        right_df,
        join_condition,
        "left_semi"
    )
