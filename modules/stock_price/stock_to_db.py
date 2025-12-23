import logging
import pandas as pd
from sqlalchemy import text
import FinanceDataReader as fdr
from io import StringIO

from modules.postgres.postgres_connector import get_postgres_engine

log = logging.getLogger(__name__)

TABLE_NAME = "daily_stock_price"
DEFAULT_START_DATE = "2005-01-01"


def setup_database_table():
    """
    주식 가격을 저장할 테이블을 생성합니다.
    """
    engine = get_postgres_engine()
    with engine.begin() as conn:
        conn.execute(
            text(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                open NUMERIC(12, 2),
                high NUMERIC(12, 2),
                low NUMERIC(12, 2),
                close NUMERIC(12, 2),
                volume BIGINT,
                adj_close NUMERIC(12, 6),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, date)
            );
            
            CREATE INDEX IF NOT EXISTS idx_symbol_date ON {TABLE_NAME}(symbol, date);
            CREATE INDEX IF NOT EXISTS idx_date ON {TABLE_NAME}(date);
        """)
        )
    log.info(f"Table '{TABLE_NAME}' is ready.")


def get_missing_symbols() -> list[str]:
    """
    KRX에 상장된 모든 종목과 DB에 저장된 종목을 비교하여,
    누락된 종목 리스트를 반환합니다.
    """
    engine = get_postgres_engine()

    try:
        krx_stocks = fdr.StockListing("KRX")
        all_symbols = krx_stocks["Code"].tolist()
        log.info(f"Fetched {len(all_symbols)} symbols from KRX.")
    except Exception as e:
        log.error(f"Failed to fetch stock listings from KRX: {e}")
        return []

    try:
        with engine.connect() as conn:
            ingested_symbols_result = conn.execute(
                text(f"SELECT DISTINCT symbol FROM {TABLE_NAME}")
            )
            ingested_symbols = {row[0] for row in ingested_symbols_result}
        log.info(f"Found {len(ingested_symbols)} symbols in the database.")
    except Exception:
        log.warning(f"Could not fetch ingested symbols, assuming none exist.")
        ingested_symbols = set()

    missing_symbols = [sym for sym in all_symbols if sym not in ingested_symbols]
    log.info(f"Found {len(missing_symbols)} missing symbols to process.")

    # For testing, you can uncomment the line below to process a small subset.
    # return missing_symbols[:10]
    return missing_symbols


def bulk_upsert(df: pd.DataFrame):
    """
    DataFrame을 DB에 배치 UPSERT 수행
    """
    if df.empty:
        log.info("No data to upsert.")
        return

    engine = get_postgres_engine()
    temp_table = f"{TABLE_NAME}_temp_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S%f')}"

    # Get the DDL for the temp table from pandas, ensuring correct dialect from connection
    with engine.connect() as conn:
        schema_sql = pd.io.sql.get_schema(df, temp_table, con=conn)

    try:
        df.to_sql(temp_table, engine, if_exists="replace", index=False, method="multi")
        with engine.begin() as conn:
            conn.execute(
                text(f"""
                INSERT INTO {TABLE_NAME} (symbol, date, open, high, low, close, volume, adj_close)
                SELECT symbol, date, open, high, low, close, volume, adj_close
                FROM {temp_table}
                ON CONFLICT (symbol, date) 
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    adj_close = EXCLUDED.adj_close;
            """)
            )

        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))

        log.info(f"Successfully upserted {len(df)} rows.")

    except Exception as e:
        log.error(f"Bulk upsert failed: {e}")
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))
        except Exception as cleanup_e:
            log.error(f"Failed to drop temporary table {temp_table}: {cleanup_e}")
        raise e


def load_data_to_postgres(**kwargs):
    """
    XCom에서 DataFrame 리스트를 받아 하나로 합쳐 DB에 적재합니다.
    """
    ti = kwargs["ti"]

    processed_data = ti.xcom_pull(task_ids="fetch_and_process_stock_data")
    if processed_data is not None:
        valid_dfs = [df for df in processed_data if df is not None and not df.empty]
    else:
        valid_dfs = []

    if not valid_dfs:
        log.info("No dataframes to load.")
        return

    combined_df = pd.concat(valid_dfs, ignore_index=True)
    log.info(
        f"Combined {len(valid_dfs)} dataframes into one with {len(combined_df)} total rows."
    )

    bulk_upsert(combined_df)
