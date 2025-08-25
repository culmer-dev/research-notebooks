import enum
import polars as pl


class OptionType(enum.Enum):
    CALL = "CALL"
    PUT = "PUT"

class CleanParquetUtility():
    
    @staticmethod
    def option_aggs_parquet(option_df: pl.DataFrame) -> pl.DataFrame:
        """
        Rename columns from source parquet, add typing, and expand option ticker
        """
        
        # Rename and select only required columns
        option_df = option_df.select('ticker', 'volume', 'open', 'window_start')
        option_df = option_df.rename({'ticker': 'option_ticker', 'open': 'option_price', 'window_start': 'timestamp'})
        
        # Parse Polygon option tickers to useful columns
        regex_pattern = r"O:([A-Za-z]+)([0-9]+)([PC])([0-9]+)"
        option_df = option_df.with_columns(
            pl.col('option_ticker').str.extract(regex_pattern, 1).alias('underlying_ticker'),
            pl.col('option_ticker').str.extract(regex_pattern, 2).alias('expiration_date'),
            pl.col('option_ticker').str.extract(regex_pattern, 3).alias('option_type'),
            pl.col('option_ticker').str.extract(regex_pattern, 4).alias('strike_price'),
        )
        
        # Expiration date can sometimes include an extra int at the front (because Polygon idk) so we need to filter those out and transform to proper type
        option_df = option_df.with_columns(
            pl.when(pl.col('expiration_date').str.len_chars() == 7)
            .then(pl.col("expiration_date").str.slice(1))
            .otherwise(pl.col('expiration_date'))
            .alias('expiration_date')
        )
        
        # Convert column types
        option_df = option_df.with_columns(
            pl.col("expiration_date").map_elements(
                lambda date_str: f"20{date_str[0:2]}-{date_str[2:4]}-{date_str[4:]}",
                return_dtype=pl.self_dtype()
                ).str.to_datetime(time_unit="ns").alias("expiration_date"),
            pl.col('option_price').cast(pl.Decimal(scale=2)).alias('option_price'),
            pl.from_epoch(pl.col("timestamp"), time_unit="ns").alias("timestamp"),
            pl.col("strike_price").cast(pl.Decimal(scale=2)) / 1000.0,
            pl.col("option_type").map_elements(
                lambda option_type_str: OptionType.CALL.value if option_type_str == "C" else OptionType.PUT.value, 
                return_dtype=pl.String
            ).cast(pl.Enum([OptionType.CALL.value, OptionType.PUT.value])).alias("option_type"),
        )
        
        return option_df

    @staticmethod
    def stock_aggs_parquet(stock_df: pl.DataFrame) -> pl.DataFrame:
        """
        Works on minute and day aggs
        """
        stock_df = stock_df.select('ticker', 'volume', 'open', 'window_start')
        stock_df = stock_df.rename({'open': 'price', 'window_start': 'timestamp'})
        stock_df = stock_df.with_columns(
            pl.col('price').cast(pl.Decimal(scale=2)).alias('price'),
            pl.from_epoch(pl.col("timestamp"), time_unit="ns").alias("timestamp"),
        )
        return stock_df

    @staticmethod
    def treasury_parquet(treasury_yields_df: pl.DataFrame) -> pl.DataFrame:
        treasury_yields_df = treasury_yields_df.select(['date', 'yield_1_month', 'yield_3_month', 'yield_1_year', 'yield_2_year', 'yield_5_year', 'yield_10_year', 'yield_30_year'])
        treasury_yields_df = treasury_yields_df.rename({'yield_1_month': '1m', 'yield_3_month': '3m', 'yield_1_year': '1y', 'yield_2_year': '2y', 'yield_5_year': '5y', 'yield_10_year': '10y', 'yield_30_year': '30y'})
        treasury_yields_df = treasury_yields_df.with_columns(
            pl.col('date').str.to_datetime(time_unit="ns").alias('date'),
            pl.col('1m').cast(pl.Decimal(scale=6)) / 100.0,
            pl.col('3m').cast(pl.Decimal(scale=6)) / 100.0,
            pl.col('1y').cast(pl.Decimal(scale=6)) / 100.0,
            pl.col('2y').cast(pl.Decimal(scale=6)) / 100.0,
            pl.col('5y').cast(pl.Decimal(scale=6)) / 100.0,
            pl.col('10y').cast(pl.Decimal(scale=6)) / 100.0,
            pl.col('30y').cast(pl.Decimal(scale=6)) / 100.0,
        )
        treasury_yields_df = treasury_yields_df.with_columns(
            pl.col('1m').fill_null(pl.col('1m').median()).alias("1m"),
            pl.col('3m').fill_null(pl.col('3m').median()).alias("3m"),
            pl.col('1y').fill_null(pl.col('1y').median()).alias("1y"),
            pl.col('2y').fill_null(pl.col('2y').median()).alias("2y"),
            pl.col('5y').fill_null(pl.col('5y').median()).alias("5y"),
            pl.col('10y').fill_null(pl.col('10y').median()).alias("10y"),
            pl.col('30y').fill_null(pl.col('30y').median()).alias("30y")
        )
        return treasury_yields_df



