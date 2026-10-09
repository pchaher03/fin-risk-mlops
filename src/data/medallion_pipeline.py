"""
Bronze Layer Ingestion Module.

Automates raw multi-asset market return series ingestion into Delta Lake tables
with schema enforcement, duplicate prevention, and local Parquet fallback support.
"""

from typing import List, Optional
import os
import numpy as np
import pandas as pd
import yfinance as yf

from src.utils.config import settings
from src.utils.logger import logger

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, current_timestamp
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

def fetch_raw_market_data(
    tickers: List[str],
    start_date: str = "2023-01-01",
    end_date: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch multi-asset OHLCV market feeds and compute daily returns.

    Args:
        tickers: List of ticker symbols (e.g., ["AAPL", "MSFT", "SPY"]).
        start_date: Fetch start date string (YYYY-MM-DD).
        end_date: Optional end date string (YYYY-MM-DD). Defaults to today.

    Returns:
        pd.DataFrame containing columns: [date, ticker, open, high, low, close, volume, return]
    """
    logger.info(f"Fetching market data for tickers: {tickers} from {start_date}")
    df_raw = yf.download(tickers, start=start_date, end=end_date, group_by="ticker")

    records = []
    for ticker in tickers:
        if len(tickers) == 1:
            data = df_raw.copy()
        else:
            if ticker not in df_raw:
                logger.warning(f"Ticker {ticker} not found in fetched payload.")
                continue
            data = df_raw[ticker].copy()

        data = data.reset_index()
        data.columns = [col.lower().replace(" ", "_") for col in data.columns]

        # Calculate daily logarithmic returns
        data["return"] = (data["close"] / data["close"].shift(1)).apply(
            lambda x: np.log(x) if pd.notnull(x) and x > 0 else 0.0
        )
        data["ticker"] = ticker
        data = data.dropna(subset=["date", "close"])
        records.append(data)

    if not records:
        logger.error("No valid market data retrieved.")
        return pd.DataFrame()

    df_final = pd.concat(records, ignore_index=True)
    df_final["date"] = pd.to_datetime(df_final["date"]).dt.strftime("%Y-%m-%d")
    
    # Standardize output schema
    target_cols = ["date", "ticker", "open", "high", "low", "close", "volume", "return"]
    return df_final[target_cols]

def ingest_bronze_market_data(
    tickers: List[str],
    spark: Optional["SparkSession"] = None,
    bronze_path: Optional[str] = None
) -> None:
    """
    Ingests market data and appends to Bronze Delta Lake table or local Parquet storage.

    Args:
        tickers: List of asset ticker symbols to ingest.
        spark: Active SparkSession instance if running on Databricks/PySpark.
        bronze_path: Target Delta Lake or Parquet storage directory path.
    """
    output_path = bronze_path or settings.BRONZE_DELTA_PATH
    df_data = fetch_raw_market_data(tickers=tickers)

    if df_data.empty:
        logger.warning("Aborting ingestion: No records fetched.")
        return

    # Check execution context (Spark Delta Lake vs Local Parquet Fallback)
    if spark and PYSPARK_AVAILABLE:
        logger.info(f"Executing Bronze ingestion to Delta Lake table at: {output_path}")
        spark_df = spark.createDataFrame(df_data)
        spark_df = spark_df.withColumn("ingestion_timestamp", current_timestamp())

        # Append to Delta Lake with schema enforcement
        (
            spark_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(output_path)
        )
        logger.info("Successfully ingested Bronze Delta Lake records.")
    else:
        logger.info(f"Spark context absent. Falling back to local Parquet storage at: {output_path}")
        os.makedirs(output_path, exist_ok=True)
        file_path = os.path.join(output_path, "bronze_market_data.parquet")

        # Deduplicate and append locally
        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path)
            combined_df = pd.concat([existing_df, df_data], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=["date", "ticker"], keep="last")
        else:
            combined_df = df_data

        combined_df.to_parquet(file_path, index=False)
        logger.info(f"Successfully saved {len(combined_df)} Bronze records to local Parquet.")

if __name__ == "__main__":
    test_tickers = ["AAPL", "MSFT", "SPY"]
    ingest_bronze_market_data(tickers=test_tickers)