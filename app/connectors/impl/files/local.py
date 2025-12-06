from typing import Any, Dict, Iterator, List, Optional, Union
import os
import glob
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from fastavro import reader as avro_reader, writer as avro_writer, parse_schema

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.connectors.base import BaseConnector
from app.core.errors import (
    ConfigurationError,
    SchemaDiscoveryError,
    DataTransferError,
)
from app.core.logging import get_logger

logger = get_logger(__name__)


class LocalFileConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)
    base_path: str = Field(..., description="Base directory path")


class FileFormat(str):
    CSV = "csv"
    JSON = "json"
    JSONL = "jsonl"
    PARQUET = "parquet"
    AVRO = "avro"
    XLSX = "xlsx"
    XLS = "xls"


class LocalFileConnector(BaseConnector):

    def __init__(self, config: Dict[str, Any]):
        self._config_model: Optional[LocalFileConfig] = None
        super().__init__(config)

    def validate_config(self) -> None:
        try:
            self._config_model = LocalFileConfig.model_validate(self.config)
            os.makedirs(self._config_model.base_path, exist_ok=True)
        except Exception as e:
            raise ConfigurationError(f"Invalid Local File configuration: {e}")

    def test_connection(self) -> bool:
        return os.path.isdir(self._config_model.base_path)

    def list_assets(self, pattern: str = "*") -> List[str]:
        base = self._config_model.base_path
        try:
            return [
                os.path.relpath(f, base)
                for f in glob.glob(os.path.join(base, pattern), recursive=True)
                if os.path.isfile(f)
            ]
        except Exception as e:
            raise SchemaDiscoveryError(f"Unable to list: {e}")

    def _get_format(self, asset: str) -> FileFormat:
        return FileFormat(os.path.splitext(asset)[1][1:].lower())

    def get_schema(self, asset: str) -> Dict[str, Any]:
        file_path = os.path.join(self._config_model.base_path, asset)
        fmt = self._get_format(asset)

        if not os.path.exists(file_path):
            raise SchemaDiscoveryError(f"File not found: {asset}")

        try:
            if fmt == FileFormat.PARQUET:
                table = pq.read_table(file_path)
                df = table.to_pandas().head(20)

            elif fmt in [FileFormat.JSON, FileFormat.JSONL]:
                df = (
                    pl.read_json(file_path, infer_schema_length=2000)
                    .head(20)
                    .to_pandas()
                )

            elif fmt == FileFormat.CSV:
                df = pl.read_csv(file_path).head(20).to_pandas()

            elif fmt in [FileFormat.XLS, FileFormat.XLSX]:
                df = pd.read_excel(file_path, nrows=20)

            elif fmt == FileFormat.AVRO:
                with open(file_path, "rb") as f:
                    rows = []
                    for i, r in enumerate(avro_reader(f)):
                        rows.append(r)
                        if i >= 19:
                            break
                df = pd.DataFrame(rows)

            else:
                raise SchemaDiscoveryError(f"Unsupported format: {fmt}")

            return {
                "asset_name": asset,
                "columns": [
                    {"name": col, "type": str(dtype)}
                    for col, dtype in df.dtypes.items()
                ],
            }
        except Exception as e:
            raise SchemaDiscoveryError(f"Schema inference failed: {e}")

    def read_batch(
        self,
        asset: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        chunksize: int = 50000,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:

        file_path = os.path.join(self._config_model.base_path, asset)
        fmt = self._get_format(asset)

        if not os.path.exists(file_path):
            raise DataTransferError(f"Missing file: {file_path}")

        try:
            if fmt == FileFormat.PARQUET:
                table = pq.read_table(file_path)
                df = table.to_pandas()

                df = self._slice(df, offset, limit)
                yield from self._chunk(df, chunksize)
                return

            if fmt == FileFormat.AVRO:
                rows = []
                with open(file_path, "rb") as f:
                    for r in avro_reader(f):
                        rows.append(r)
                df = pd.DataFrame(rows)

                df = self._slice(df, offset, limit)
                yield from self._chunk(df, chunksize)
                return

            if fmt in [FileFormat.JSON, FileFormat.JSONL]:
                df = pl.read_ndjson(file_path).to_pandas()
                df = self._slice(df, offset, limit)
                yield from self._chunk(df, chunksize)
                return

            if fmt == FileFormat.CSV:
                df = pl.read_csv(file_path).to_pandas()
                df = self._slice(df, offset, limit)
                yield from self._chunk(df, chunksize)
                return

            if fmt in [FileFormat.XLS, FileFormat.XLSX]:
                df = pd.read_excel(file_path)
                df = self._slice(df, offset, limit)
                yield from self._chunk(df, chunksize)
                return

            raise DataTransferError(f"Unsupported format: {fmt}")

        except Exception as e:
            raise DataTransferError(f"Read failed: {e}")

    # ---------------------------------------------------------
    def _slice(self, df: pd.DataFrame, offset: Optional[int], limit: Optional[int]):
        if offset:
            df = df.iloc[offset:]
        if limit:
            df = df.iloc[:limit]
        return df

    def _chunk(self, df: pd.DataFrame, chunksize: int):
        for i in range(0, len(df), chunksize):
            yield df.iloc[i : i + chunksize]

    def write_batch(
        self,
        data: Union[pd.DataFrame, Iterator[pd.DataFrame]],
        asset: str,
        mode: str = "append",
        **kwargs,
    ) -> int:

        file_path = os.path.join(self._config_model.base_path, asset)
        fmt = self._get_format(asset)

        if isinstance(data, pd.DataFrame):
            data = [data]

        if mode == "replace" and os.path.exists(file_path):
            os.remove(file_path)

        rows_written = 0

        try:
            for df in data:
                if df.empty:
                    continue

                if fmt == FileFormat.PARQUET:
                    table = pa.Table.from_pandas(df)

                    if not os.path.exists(file_path):
                        pq.write_table(table, file_path)
                    else:
                        pq.write_table(table, file_path, append=True)
                elif fmt == FileFormat.AVRO:
                    schema = {
                        "type": "record",
                        "name": "row",
                        "fields": [
                            {"name": col, "type": "string"} for col in df.columns
                        ],
                    }
                    parsed = parse_schema(schema)
                    with open(file_path, "ab") as f:
                        avro_writer(f, parsed, df.to_dict("records"))

                elif fmt == FileFormat.CSV:
                    df.to_csv(
                        file_path,
                        mode="a",
                        index=False,
                        header=not os.path.exists(file_path),
                    )

                elif fmt in [FileFormat.JSON, FileFormat.JSONL]:
                    df.to_json(
                        file_path,
                        orient="records",
                        lines=True,
                        mode="a",
                    )

                elif fmt in [FileFormat.XLS, FileFormat.XLSX]:
                    df.to_excel(file_path, index=False)

                else:
                    raise DataTransferError(f"Write format not supported: {fmt}")

                rows_written += len(df)

            return rows_written

        except Exception as e:
            raise DataTransferError(f"Write failed: {e}")