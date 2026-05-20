from pyspark.sql import DataFrame, SparkSession

def create_iceberg_table(spark: SparkSession, table_name: str, schema: str, partition_by: list = None):
    partition_sql = ""

    if partition_by:
        partition_sql = f"PARTITIONED BY ({', '.join(partition_by)})"

    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {schema}
    )
    USING iceberg
    {partition_sql}
    """

    spark.sql(query)


def append_data(df: DataFrame, table_name: str):
    df.write.format("iceberg").mode("append").saveAsTable(table_name)


def overwrite_table(df: DataFrame, table_name: str):
    df.write.format("iceberg").mode("overwrite").saveAsTable(table_name)


def safe_write(df: DataFrame, table_name: str, mode: str = "append"):
    df.write.format("iceberg").mode(mode).saveAsTable(table_name)


def overwrite_partition(df: DataFrame, table_name: str):
    df.write.format("iceberg") \
        .mode("overwrite") \
        .option("replace-partitions", "true") \
        .saveAsTable(table_name)

def create_temp_view(df: DataFrame, view_name: str):
    df.createOrReplaceTempView(view_name)


def merge_into_iceberg(
    spark: SparkSession,
    source_view: str,
    target_table: str,
    merge_condition: str,
    update_set: dict,
    insert_set: dict
):

    update_expr = ", ".join([f"t.{k} = s.{v}" for k, v in update_set.items()])
    insert_cols = ", ".join(insert_set.keys())
    insert_vals = ", ".join([f"s.{v}" for v in insert_set.values()])

    query = f"""
    MERGE INTO {target_table} t
    USING {source_view} s
    ON {merge_condition}

    WHEN MATCHED THEN UPDATE SET {update_expr}

    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    spark.sql(query)



def read_snapshot(spark: SparkSession, table_name: str, snapshot_id: str):
    return spark.sql(f"""
        SELECT * FROM {table_name}
        FOR SYSTEM_VERSION AS OF '{snapshot_id}'
    """)


def read_by_timestamp(spark: SparkSession, table_name: str, timestamp: str):
    return spark.sql(f"""
        SELECT * FROM {table_name}
        FOR SYSTEM_TIME AS OF TIMESTAMP '{timestamp}'
    """)


def show_snapshots(spark: SparkSession, table_name: str):
    return spark.sql(f"SELECT * FROM {table_name}.snapshots")


def show_history(spark: SparkSession, table_name: str):
    return spark.sql(f"SELECT * FROM {table_name}.history")


def delete_from_table(spark: SparkSession, table_name: str, condition: str):
    spark.sql(f"""
        DELETE FROM {table_name}
        WHERE {condition}
    """)




def iceberg_maintenance(
    spark: SparkSession,
    table_name: str,
    mode: str = "full",
    snapshot_retention_hours: int = 24 * 7  # default 7 days
):
    """
    Iceberg maintenance utility with 3 strategies:

    Modes:
    -----------------------------------------
    compact            -> rewrite data files (performance)
    cleanup            -> remove orphan files (storage cleanup)
    expire_snapshots   -> remove old snapshots (time travel cleanup)
    full               -> run all 3
    """

    mode = mode.lower().strip()

    # -------------------------------------------------
    # 1. COMPACTION (performance optimization)
    # -------------------------------------------------
    def compact():
        spark.sql(f"""
            CALL system.rewrite_data_files(
                table => '{table_name}'
            )
        """)

    # -------------------------------------------------
    # 2. CLEANUP (remove orphan files)
    # -------------------------------------------------
    def cleanup():
        spark.sql(f"""
            CALL system.remove_orphan_files(
                table => '{table_name}'
            )
        """)

    # -------------------------------------------------
    # 3. EXPIRE SNAPSHOTS (time travel cleanup)
    # -------------------------------------------------
    def expire_snapshots():
        spark.sql(f"""
            CALL system.expire_snapshots(
                table => '{table_name}',
                retain_last => 5
            )
        """)

    # -------------------------------------------------
    # MODE ROUTING
    # -------------------------------------------------

    if mode == "compact":
        compact()

    elif mode == "cleanup":
        cleanup()

    elif mode == "expire_snapshots":
        expire_snapshots()

    elif mode == "full":
        compact()
        expire_snapshots()
        cleanup()

    else:
        raise ValueError(
            "Invalid mode. Use: compact | cleanup | expire_snapshots | full"
        )
