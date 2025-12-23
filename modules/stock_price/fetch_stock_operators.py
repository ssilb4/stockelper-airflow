from __future__ import annotations

import logging
from datetime import datetime

import FinanceDataReader as fdr
import pandas as pd
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

log = logging.getLogger(__name__)

DEFAULT_START_DATE = "2005-01-01"


class FetchStockDataOperator(BaseOperator):
    """
    단일 주식 종목의 데이터를 가져와서 전처리하고 XCom으로 DataFrame을 반환하는 오퍼레이터
    """

    template_fields: tuple[str, ...] = ("symbol",)

    def __init__(self, symbol: str, **kwargs):
        super().__init__(**kwargs)
        self.symbol = symbol

    def execute(self, context: Context) -> pd.DataFrame | None:
        """
        symbol에 해당하는 주식 데이터를 가져와 전처리 후 DataFrame을 반환합니다.
        """
        log.info(f"Fetching and processing data for symbol: {self.symbol}")
        to_date = datetime.now().strftime("%Y-%m-%d")

        try:
            krx_df = fdr.DataReader(
                f"KRX:{self.symbol}", start=DEFAULT_START_DATE, end=to_date
            )
            yh_df = fdr.DataReader(
                f"YAHOO:{self.symbol}.KS", start=DEFAULT_START_DATE, end=to_date
            )

            df = yh_df.merge(
                krx_df,
                left_index=True,
                right_index=True,
                suffixes=("", "_krx"),
                how="outer",
            )
            df[["Open", "High", "Low", "Close", "Volume", "Adj Close"]] = df[
                ["Open", "High", "Low", "Close", "Volume", "Adj Close"]
            ].bfill()
            df.dropna(inplace=True)
            df = df[["Open", "High", "Low", "Close", "Volume", "Adj Close"]]

        except Exception as e:
            if "Not Found for url" in str(e) or isinstance(e, KeyError):
                log.warning(
                    f"Could not find Yahoo data for {self.symbol}. Using KRX data only. Error: {e}"
                )
                try:
                    krx_df = fdr.DataReader(
                        f"KRX:{self.symbol}", start=DEFAULT_START_DATE, end=to_date
                    )
                    df = krx_df[["Open", "High", "Low", "Close", "Volume"]].copy()
                    df["Adj Close"] = df["Close"]
                except Exception as krx_e:
                    log.error(
                        f"Failed to fetch even KRX data for {self.symbol}: {krx_e}"
                    )
                    return df
            else:
                log.error(
                    f"An unexpected error occurred while fetching data for {self.symbol}: {e}"
                )
                return df

        if df.empty:
            log.warning(f"No data found for symbol {self.symbol} after processing.")
            return df

        df.index.name = "Date"
        df.reset_index(inplace=True)
        df["symbol"] = self.symbol

        df = df.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Adj Close": "adj_close",
                "Date": "date",
            }
        )

        df = df[
            ["symbol", "date", "open", "high", "low", "close", "volume", "adj_close"]
        ]

        log.info(f"Processed {len(df)} rows for symbol {self.symbol}.")
        return df
