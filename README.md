# spark

## AnalyticalUtils

- The add_rank function is a reusable PySpark utility used to generate ranked values within partitions of a DataFrame.
### 
```python
def add_rank(
    df: DataFrame,
    partition_cols: list,
    order_col: str,
    order: str = "desc"
):
```
| Parameter        | Type      | Description |
|----------------|----------|-------------|
| df             | DataFrame | Input Spark DataFrame |
| partition_cols | list      | Columns used to define partition/grouping |
| order_col      | str       | Column used to determine ranking order |
| order          | str       | Sorting order: `"asc"` or `"desc"` (default: `"desc"`) |

### How to use 
```python
df_ranked = add_rank(df, ["customer_id"], "transaction_date", "desc")
df_latest = df_ranked.filter("rank = 1")
```
- A new column name `rank` will be added in the dataframe.



- The add_dense_rank function is a reusable PySpark utility used to generate dense ranked values within partitions of a DataFrame.

### 
```python
def add_dense_rank(
    df: DataFrame,
    partition_cols: list,
    order_col: str,
    order: str = "desc"
):
```

| Parameter        | Type      | Description |
|----------------|----------|-------------|
| df             | DataFrame | Input Spark DataFrame |
| partition_cols | list      | Columns used to define partition/grouping |
| order_col      | str       | Column used to determine ranking order |
| order          | str       | Sorting order: `"asc"` or `"desc"` (default: `"desc"`) |

### How to use 
```python
df_ranked = add_rank(df, ["customer_id"], "transaction_date", "desc")
df_latest = df_ranked.filter("rank = 1")
```
- A new column name `dense_rank` will be added in the dataframe.


### running_total 
- The running_total function is a reusable PySpark utility used to calculate cumulative sum (running total) within partitions of a DataFrame based on a specified ordering column.

```python
def running_total(
    df: DataFrame,
    partition_cols: list,
    order_col: str,
    value_col: str,
    order: str = "asc"
):
```

| Parameter      | Type      | Description                                           |
| -------------- | --------- | ----------------------------------------------------- |
| df             | DataFrame | Input Spark DataFrame                                 |
| partition_cols | list      | Columns used to define partition/grouping             |
| order_col      | str       | Column used to determine ordering for running total   |
| value_col      | str       | Column whose values will be aggregated cumulatively   |
| order          | str       | Sorting order: `"asc"` or `"desc"` (default: `"asc"`) |


