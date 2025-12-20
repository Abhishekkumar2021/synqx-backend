from typing import Any, Dict, List, Optional, Iterator, Union
import os
import glob
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from fastavro import reader as avro_reader, writer as avro_writer, parse_schema
from datetime import datetime

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


# ---------------------------------------------------------
# File Format Constants
# ---------------------------------------------------------
class FileFormat(str):
    CSV = "csv"
    JSON = "json"
    JSONL = "jsonl"
    PARQUET = "parquet"
    AVRO = "avro"
    XLSX = "xlsx"
    XLS = "xls"


# ---------------------------------------------------------
# Config model
# ---------------------------------------------------------
class LocalFileConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)
    base_path: str = Field(..., description="Base directory for files")


# ---------------------------------------------------------
# Unified Local File Connector
# ---------------------------------------------------------
class LocalFileConnector(BaseConnector):

    def __init__(self, config: Dict[str, Any]):
        self._config_model: Optional[LocalFileConfig] = None
        super().__init__(config)

    def validate_config(self):
        try:
            self._config_model = LocalFileConfig.model_validate(self.config)
            os.makedirs(self._config_model.base_path, exist_ok=True)
        except Exception as e:
            raise ConfigurationError(f"Invalid LocalFile configuration: {e}")

    def connect(self) -> None:
        """Local filesystem does not require real connection."""
        return

    def disconnect(self) -> None:
        return

    def test_connection(self) -> bool:
        return os.path.isdir(self._config_model.base_path)

    def _full_path(self, asset: str) -> str:
        return os.path.join(self._config_model.base_path, asset)

    def _detect_format(self, asset: str) -> FileFormat:
        ext = os.path.splitext(asset)[1][1:].lower()
        return FileFormat(ext)

    def discover_assets(
        self, pattern: Optional[str] = None, include_metadata: bool = False, **kwargs
    ) -> List[Dict[str, Any]]:

        base = self._config_model.base_path
        pattern = pattern or "*"

        files = [
            os.path.relpath(f, base)
            for f in glob.glob(os.path.join(base, pattern), recursive=True)
            if os.path.isfile(f)
        ]

        if not include_metadata:
            return [{"name": f} for f in files]

        # Build metadata-rich response
        results = []
        for f in files:
            fp = self._full_path(f)
            stat = os.stat(fp)

            results.append(
                {
                    "name": f,
                    "type": self._detect_format(f),
                    "size_bytes": stat.st_size,
                    "row_count": self._estimate_row_count(f),
                    "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                }
            )

        return results

    def _estimate_row_count(self, asset: str) -> Optional[int]:
        """Cheap row estimate when possible."""
        fmt = self._detect_format(asset)
        fp = self._full_path(asset)

        try:
            if fmt == FileFormat.CSV:
                with open(fp, "r", encoding="utf-8") as f:
                    return sum(1 for _ in f) - 1  # minus header
            if fmt in [FileFormat.JSONL]:
                with open(fp, "r", encoding="utf-8") as f:
                    return sum(1 for _ in f)
            if fmt == FileFormat.PARQUET:
                tbl = pq.read_table(fp)
                return tbl.num_rows
        except Exception:
            return None

        return None

    def infer_schema(
        self, asset: str, sample_size: int = 1000, mode: str = "auto", **kwargs
    ) -> Dict[str, Any]:

        fmt = self._detect_format(asset)

        # metadata-based → Parquet, Avro, Excel
        if mode == "metadata":
            return self._schema_from_metadata(asset, fmt)

        # sample-based
        if mode == "sample":
            return self._schema_from_sample(asset, sample_size)

        # AUTO: metadata first, fallback to sample
        try:
            return self._schema_from_metadata(asset, fmt)
        except Exception:
            return self._schema_from_sample(asset, sample_size)

    def _schema_from_metadata(self, asset: str, fmt: FileFormat) -> Dict[str, Any]:
        fp = self._full_path(asset)

        try:
            if fmt == FileFormat.PARQUET:
                tbl = pq.read_table(fp)
                return {
                    "asset": asset,
                    "columns": [
                        {"name": name, "type": str(tbl.schema.field(name).type)}
                        for name in tbl.schema.names
                    ],
                }

            if fmt == FileFormat.AVRO:
                with open(fp, "rb") as f:
                    av_reader = avro_reader(f)
                    schema = av_reader.schema
                fields = [
                    {"name": field["name"], "type": field["type"]}
                    for field in schema["fields"]
                ]
                return {"asset": asset, "columns": fields}

            if fmt in [FileFormat.XLS, FileFormat.XLSX]:
                df = pd.read_excel(fp, nrows=5)
                return {
                    "asset": asset,
                    "columns": [
                        {"name": col, "type": str(dtype)}
                        for col, dtype in df.dtypes.items()
                    ],
                }

            # CSV/JSON have no metadata schema → handled in sample
            raise SchemaDiscoveryError("Metadata schema not available for this format.")

        except Exception as e:
            raise SchemaDiscoveryError(f"Metadata schema extraction failed: {e}")

    def _schema_from_sample(self, asset: str, sample_size: int) -> Dict[str, Any]:
        try:
            # Use read_batch to get the first chunk as sample
            df_iter = self.read_batch(asset, limit=sample_size)
            df = next(df_iter)
            return {
                "asset": asset,
                "columns": [
                    {"name": col, "type": str(dtype)}
                    for col, dtype in df.dtypes.items()
                ],
            }
        except Exception as e:
            raise SchemaDiscoveryError(f"Sample-based schema failed: {e}")

    def read_batch(
        self,
        asset: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        chunksize: int = 50000,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:

        fp = self._full_path(asset)
        fmt = self._detect_format(asset)

        if not os.path.exists(fp):
            raise DataTransferError(f"File not found: {fp}")

        try:
            if fmt == FileFormat.PARQUET:
                table = pq.read_table(fp).to_pandas()
                df = self.slice_dataframe(table, offset, limit)
                yield from self.chunk_dataframe(df, chunksize)
                return

            if fmt == FileFormat.AVRO:
                rows = []
                with open(fp, "rb") as f:
                    for r in avro_reader(f):
                        rows.append(r)
                df = pd.DataFrame(rows)
                df = self.slice_dataframe(df, offset, limit)
                yield from self.chunk_dataframe(df, chunksize)
                return

            if fmt in [FileFormat.JSON, FileFormat.JSONL]:
                df = pl.read_ndjson(fp).to_pandas()
                df = self.slice_dataframe(df, offset, limit)
                yield from self.chunk_dataframe(df, chunksize)
                return

            if fmt == FileFormat.CSV:
                df = pl.read_csv(fp).to_pandas()
                df = self.slice_dataframe(df, offset, limit)
                yield from self.chunk_dataframe(df, chunksize)
                return

            if fmt in [FileFormat.XLS, FileFormat.XLSX]:
                df = pd.read_excel(fp)
                df = self.slice_dataframe(df, offset, limit)
                yield from self.chunk_dataframe(df, chunksize)
                return

            raise DataTransferError(f"Unsupported file format: {fmt}")

        except Exception as e:
            raise DataTransferError(f"Read failed: {e}")

    def write_batch(
        self,
        data: Union[pd.DataFrame, Iterator[pd.DataFrame]],
        asset: str,
        mode: str = "append",
        **kwargs,
    ) -> int:

        fp = self._full_path(asset)
        fmt = self._detect_format(asset)

        if isinstance(data, pd.DataFrame):
            data = [data]

        # Replace mode clears file
        if mode == "replace" and os.path.exists(fp):
            os.remove(fp)

        total = 0

        try:
            for df in data:
                if df.empty:
                    continue

                if fmt == FileFormat.PARQUET:
                    table = pa.Table.from_pandas(df)
                    if not os.path.exists(fp) or mode == "replace":
                        pq.write_table(table, fp)
                    else:
                        # Proper append using ParquetWriter
                        with pq.ParquetWriter(
                            fp, table.schema, use_dictionary=True
                        ) as writer:
                            writer.write_table(table)

                elif fmt == FileFormat.AVRO:
                    schema = {
                        "type": "record",
                        "name": "row",
                        "fields": [
                            {"name": col, "type": "string"} for col in df.columns
                        ],
                    }
                    parsed = parse_schema(schema)
                    with open(fp, "ab") as f:
                        avro_writer(f, parsed, df.to_dict("records"))

                elif fmt == FileFormat.CSV:
                    df.to_csv(fp, mode="a", index=False, header=not os.path.exists(fp))

                elif fmt in [FileFormat.JSON, FileFormat.JSONL]:
                    df.to_json(fp, orient="records", lines=True, mode="a")

                elif fmt in [FileFormat.XLS, FileFormat.XLSX]:
                    df.to_excel(fp, index=False)

                else:
                    raise DataTransferError(f"Unsupported format: {fmt}")

                total += len(df)

            return total

        except Exception as e:
            raise DataTransferError(f"Write failed: {e}")
