from __future__ import annotations

import logging
import os
from typing import Dict, Tuple

from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import (
    avg,
    col,
    count,
    count_distinct,
    lit,
    month,
    monthname,
    sum as sf_sum,
    year,
    when,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)

DB = os.getenv("SNOWFLAKE_DATABASE", "RETAIL_SALES_DB")
RAW_SCHEMA = os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")
ANALYTICS_SCHEMA = os.getenv("SNOWFLAKE_ANALYTICS_SCHEMA", "ANALYTICS")
RAW_TABLE = f"{DB}.{RAW_SCHEMA}.RAW_SALES"

REQUIRED_COLUMNS = {
    "DATE",
    "PRODUCT",
    "REGION",
    "UNITS_SOLD",
    "UNIT_PRICE",
    "SALES",
}


def create_session() -> Session:
    
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Missing Snowflake environment variables: {', '.join(missing)}")

    configs = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "database": DB,
        "schema": RAW_SCHEMA,
    }
    if os.getenv("SNOWFLAKE_WAREHOUSE"):
        configs["warehouse"] = os.environ["SNOWFLAKE_WAREHOUSE"]
    if os.getenv("SNOWFLAKE_ROLE"):
        configs["role"] = os.environ["SNOWFLAKE_ROLE"]

    return Session.builder.configs(configs).create()


def read_raw_data(session: Session) -> DataFrame:
    
    LOGGER.info("Reading %s", RAW_TABLE)
    return session.table(RAW_TABLE)


def validate_schema(df: DataFrame) -> None:
    
    actual = {name.upper() for name in df.columns}
    missing = REQUIRED_COLUMNS - actual
    if missing:
        raise ValueError(
            "RAW_SALES is missing required columns: " + ", ".join(sorted(missing))
        )


def clean_data(df: DataFrame) -> DataFrame:
    
    cleaned = (
        df.select(
            col("DATE").cast("DATE").alias("DATE"),
            col("PRODUCT").cast("STRING").alias("PRODUCT"),
            col("REGION").cast("STRING").alias("REGION"),
            col("UNITS_SOLD").cast("NUMBER(18,0)").alias("UNITS_SOLD"),
            col("UNIT_PRICE").cast("NUMBER(18,2)").alias("UNIT_PRICE"),
            col("SALES").cast("NUMBER(18,2)").alias("SALES"),
        )
        .filter(col("DATE").is_not_null())
        .filter(col("PRODUCT").is_not_null())
        .filter(col("REGION").is_not_null())
        .filter(col("UNITS_SOLD").is_not_null())
        .filter(col("UNIT_PRICE").is_not_null())
        .filter(col("UNITS_SOLD") >= lit(0))
        .filter(col("UNIT_PRICE") >= lit(0))
        .drop_duplicates()
    )
    return cleaned


def add_derived_columns(df: DataFrame) -> DataFrame:
   
    return (
        df.with_column("TOTAL_SALES", (col("UNITS_SOLD") * col("UNIT_PRICE")).cast("NUMBER(18,2)"))
        .with_column("YEAR", year(col("DATE")))
        .with_column("MONTH", month(col("DATE")))
        .with_column("MONTH_NAME", monthname(col("DATE")))
        .with_column(
            "SALES_VARIANCE",
            (col("SALES") - col("TOTAL_SALES")).cast("NUMBER(18,2)"),
        )
    )


def build_product_sales(df: DataFrame) -> DataFrame:
    return (
        df.group_by("PRODUCT")
        .agg(
            sf_sum("TOTAL_SALES").alias("TOTAL_REVENUE"),
            sf_sum("UNITS_SOLD").alias("TOTAL_UNITS_SOLD"),
            count("*").alias("TRANSACTION_COUNT"),
        )
        .with_column("RANK", __import__("snowflake.snowpark.functions", fromlist=["row_number"]).row_number().over(
            __import__("snowflake.snowpark.window", fromlist=["Window"]).Window.order_by(col("TOTAL_REVENUE").desc())
        ))
    )


def build_regional_sales(df: DataFrame) -> DataFrame:
    return (
        df.group_by("REGION")
        .agg(
            sf_sum("TOTAL_SALES").alias("TOTAL_REVENUE"),
            sf_sum("UNITS_SOLD").alias("TOTAL_UNITS_SOLD"),
            count("*").alias("TRANSACTION_COUNT"),
        )
        .sort(col("TOTAL_REVENUE").desc())
    )


def build_monthly_sales(df: DataFrame) -> DataFrame:
    return (
        df.group_by("YEAR", "MONTH", "MONTH_NAME")
        .agg(
            sf_sum("TOTAL_SALES").alias("TOTAL_REVENUE"),
            sf_sum("UNITS_SOLD").alias("TOTAL_UNITS_SOLD"),
            count("*").alias("TRANSACTION_COUNT"),
        )
        .sort(col("YEAR"), col("MONTH"))
    )


def build_product_performance(df: DataFrame) -> DataFrame:
    return (
        df.group_by("PRODUCT")
        .agg(
            sf_sum("UNITS_SOLD").alias("TOTAL_UNITS_SOLD"),
            sf_sum("TOTAL_SALES").alias("TOTAL_REVENUE"),
            avg("UNIT_PRICE").alias("AVERAGE_UNIT_PRICE"),
            count("*").alias("TRANSACTION_COUNT"),
        )
        .with_column(
            "REVENUE_PER_UNIT",
            when(col("TOTAL_UNITS_SOLD") != 0,
                 col("TOTAL_REVENUE") / col("TOTAL_UNITS_SOLD"))
            .otherwise(lit(0)),
        )
        .sort(col("TOTAL_REVENUE").desc())
    )


def build_overall_metrics(df: DataFrame) -> DataFrame:
    
    return df.agg(
        sf_sum("TOTAL_SALES").alias("TOTAL_REVENUE"),
        sf_sum("UNITS_SOLD").alias("TOTAL_UNITS_SOLD"),
        count_distinct("DATE").alias("ACTIVE_DAYS"),
        count("*").alias("TRANSACTION_COUNT"),
    ).with_column(
        "REVENUE_PER_UNIT",
        when(col("TOTAL_UNITS_SOLD") != 0,
             col("TOTAL_REVENUE") / col("TOTAL_UNITS_SOLD"))
        .otherwise(lit(0)),
    ).with_column(
        "TRADITIONAL_AOV",
        when(col("TRANSACTION_COUNT") != 0,
             col("TOTAL_REVENUE") / col("TRANSACTION_COUNT"))
        .otherwise(lit(0)),
    )


def write_table(df: DataFrame, table_name: str) -> None:
    
    full_name = f"{DB}.{ANALYTICS_SCHEMA}.{table_name}"
    LOGGER.info("Writing %s", full_name)
    df.write.mode("overwrite").save_as_table(full_name)


def run_pipeline(session: Session) -> Dict[str, int]:
    
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {DB}.{ANALYTICS_SCHEMA}").collect()

    raw = read_raw_data(session)
    validate_schema(raw)
    cleaned = clean_data(raw)
    analytics = add_derived_columns(cleaned)

    write_table(analytics, "SALES_ANALYTICS")
    write_table(build_product_sales(analytics), "PRODUCT_SALES")
    write_table(build_regional_sales(analytics), "REGIONAL_SALES")
    write_table(build_monthly_sales(analytics), "MONTHLY_SALES")
    write_table(build_product_performance(analytics), "PRODUCT_PERFORMANCE")
    write_table(build_overall_metrics(analytics), "OVERALL_METRICS")

    count_value = analytics.count()
    LOGGER.info("Pipeline completed successfully. Analytics rows: %s", count_value)
    return {"analytics_rows": count_value}


def main() -> None:
    session = None
    try:
        session = create_session()
        result = run_pipeline(session)
        LOGGER.info("Result: %s", result)
    except Exception:
        LOGGER.exception("Retail sales pipeline failed")
        raise
    finally:
        if session:
            session.close()


if __name__ == "__main__":
    main()
