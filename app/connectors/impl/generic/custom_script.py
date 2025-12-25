from typing import Any, Dict, List, Optional, Iterator
import pandas as pd
import numpy as np
import subprocess
import tempfile
import os
import json
import inspect
import random
from app.connectors.base import BaseConnector
from app.core.errors import (
    ConfigurationError,
    DataTransferError,
    SchemaDiscoveryError,
)
from app.core.logging import get_logger

logger = get_logger(__name__)

class CustomScriptConnector(BaseConnector):
    """
    Connector for executing custom scripts (Python, Shell) to extract data.
    The 'Asset' defines the script code and language.
    """

    def __init__(self, config: Dict[str, Any]):
        self.base_path = config.get("base_path", "/tmp")
        self.env_vars = config.get("env_vars", {})
        super().__init__(config)

    def validate_config(self) -> None:
        # Connection config might be empty or just env vars
        pass

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def test_connection(self) -> bool:
        """
        Tests if the execution environment is ready.
        Tries to run a simple python command.
        """
        try:
            # Simple check if we can run python code
            test_code = "print('ok')"
            # Reuse execute logic or just run subprocess directly for speed
            # Since we have _execute_python, let's try to verify imports work
            local_scope = {"pd": pd, "np": np}
            exec("import pandas as pd; import numpy as np", local_scope)
            return True
        except Exception as e:
            logger.error(f"Custom Script environment check failed: {e}")
            return False

    def discover_assets(
        self,
        pattern: Optional[str] = None,
        include_metadata: bool = False,
        **kwargs
    ) -> List[Dict[str, Any]]:
        # Discovery is not primary mode for this connector
        return []

    def infer_schema(
        self,
        asset: str,
        sample_size: int = 1000,
        **kwargs
    ) -> Dict[str, Any]:
        try:
            df = next(self.read_batch(asset, limit=10, **kwargs))
            columns = [{"name": col, "type": str(dtype)} for col, dtype in df.dtypes.items()]
            return {"asset": asset, "columns": columns}
        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to infer schema for script '{asset}': {e}")

    def read_batch(
        self,
        asset: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs
    ) -> Iterator[pd.DataFrame]:
        
        code = kwargs.get("code") or kwargs.get("query")
        language = kwargs.get("language", "python")
        
        if not code:
            possible_path = os.path.join(self.base_path, asset)
            if os.path.exists(possible_path):
                if possible_path.endswith(".py"):
                    language = "python"
                    with open(possible_path, "r") as f: code = f.read()
                elif possible_path.endswith(".sh"):
                    language = "shell"
                    code = possible_path
            else:
                raise DataTransferError(f"No code provided for asset '{asset}' and file not found.")

        incremental_filter = kwargs.get("incremental_filter")
        
        # Clean up kwargs to avoid multiple values for arguments
        exec_kwargs = kwargs.copy()
        for key in ["code", "query", "language", "incremental_filter"]:
            exec_kwargs.pop(key, None)

        if language == "python":
            yield from self._execute_python(asset, code, limit, offset, incremental_filter=incremental_filter, **exec_kwargs)
        elif language == "shell":
            yield from self._execute_shell(asset, code, limit, offset, incremental_filter=incremental_filter, **exec_kwargs)
        else:
            raise ConfigurationError(f"Unsupported script language: {language}")

    def _execute_python(self, asset_name: str, code: str, limit: int, offset: int, incremental_filter: Optional[Dict] = None, **kwargs) -> Iterator[pd.DataFrame]:
        """
        Executes Python code in-process. 
        """
        from datetime import datetime, timedelta
        local_scope = {
            "pd": pd,
            "pandas": pd,
            "json": json,
            "datetime": datetime,
            "timedelta": timedelta,
            "random": random,
            "os": os,
            "np": numpy,
            "numpy": numpy,
        }
        
        try:
            exec(code, local_scope)
        except Exception as e:
            raise DataTransferError(f"Failed to compile/execute Python script: {e}")

        # Find the entry point
        func = local_scope.get(asset_name) or local_scope.get("extract") or local_scope.get("main")
        
        if not func or not callable(func):
            # Fallback: Did the script produce a variable named 'data' or 'df'?
            # We wrap this in an iterator to apply filtering below consistently
            def variable_yielder():
                if "df" in local_scope and isinstance(local_scope["df"], pd.DataFrame):
                    yield local_scope["df"]
                elif "data" in local_scope and isinstance(local_scope["data"], list):
                    yield pd.DataFrame(local_scope["data"])
                else:
                    raise DataTransferError(f"No callable function ('{asset_name}', 'extract', 'main') or 'df'/'data' variable found in script.")
            
            result_iter = variable_yielder()
            filter_consumed = False # Variable approach can't consume args
        else:
            # Call the function
            try:
                # Inspect signature to pass args if accepted
                sig = inspect.signature(func)
                call_args = {}
                if 'limit' in sig.parameters: call_args['limit'] = limit
                if 'offset' in sig.parameters: call_args['offset'] = offset
                if 'incremental_filter' in sig.parameters: call_args['incremental_filter'] = incremental_filter
                
                # Pass kwargs too if they match
                for k,v in kwargs.items():
                    if k in sig.parameters: call_args[k] = v

                # Check if the user function accepted the filter
                filter_consumed = 'incremental_filter' in call_args

                result = func(**call_args)
                
                # Normalize result to iterator
                if isinstance(result, pd.DataFrame):
                    result_iter = iter([result])
                elif isinstance(result, list):
                    result_iter = iter([pd.DataFrame(result)])
                elif isinstance(result, Iterator):
                    result_iter = result
                else:
                    result_iter = iter([pd.DataFrame([result])])

            except Exception as e:
                raise DataTransferError(f"Error during Python execution: {e}")

        # Yield results, applying fallback filtering if needed
        for chunk in result_iter:
            df = chunk if isinstance(chunk, pd.DataFrame) else pd.DataFrame(chunk)
            
            if not filter_consumed and incremental_filter and isinstance(incremental_filter, dict):
                # Fallback: Apply filter in memory since script didn't accept it
                for col, val in incremental_filter.items():
                    # Ensure val is a scalar
                    if isinstance(val, (list, tuple, np.ndarray)) and len(val) > 0:
                        val = val[0]
                    elif isinstance(val, dict) and len(val) > 0:
                        val = next(iter(val.values()))

                    if col in df.columns:
                        series = df[col]
                        try:
                            if pd.api.types.is_numeric_dtype(series):
                                # Robust numeric conversion
                                if isinstance(val, str):
                                    threshold = float(val.strip().replace(',', ''))
                                else:
                                    threshold = float(val)
                                
                                # Use .values for reliability
                                df = df[series.values > threshold]
                            elif pd.api.types.is_datetime64_any_dtype(series):
                                threshold = pd.to_datetime(val)
                                if series.dt.tz is not None and threshold.tzinfo is None:
                                    threshold = threshold.replace(tzinfo=timezone.utc)
                                df = df[pd.to_datetime(series) > threshold]
                            else:
                                # String comparison fallback
                                df = df[series.astype(str).values > str(val)]
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Fallback filter failed for column '{col}': {e}")
            
            if not df.empty:
                yield df

    def _execute_shell(self, asset_name: str, code: str, limit: int, offset: int, incremental_filter: Optional[Dict] = None, **kwargs) -> Iterator[pd.DataFrame]:
        """
        Executes a shell command. 
        """
        is_file = os.path.exists(code) and os.path.isfile(code)
        cmd_env = os.environ.copy()
        cmd_env.update(self.env_vars)
        
        if limit: cmd_env['LIMIT'] = str(limit)
        if offset: cmd_env['OFFSET'] = str(offset)
        if incremental_filter: cmd_env['INCREMENTAL_FILTER'] = json.dumps(incremental_filter)

        if is_file:
            script_path = code
            cmd = ["bash", script_path]
        else:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as tf:
                if not code.startswith("#!"):
                    tf.write("#!/bin/bash\n")
                tf.write(code)
                script_path = tf.name
            os.chmod(script_path, 0o755)
            cmd = ["bash", script_path]

        try:
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
                env=cmd_env, text=True
            )
            
            batch = []
            chunk_size = 5000
            for line in process.stdout:
                if not line.strip(): continue
                try:
                    batch.append(json.loads(line))
                except json.JSONDecodeError: pass
                
                if len(batch) >= chunk_size:
                    yield pd.DataFrame(batch)
                    batch = []
            
            if batch: yield pd.DataFrame(batch)
            process.wait()
            if process.returncode != 0:
                stderr = process.stderr.read()
                raise DataTransferError(f"Shell script failed (Exit {process.returncode}): {stderr}")
        except Exception as e:
            raise DataTransferError(f"Shell execution error: {e}")
        finally:
            if not is_file and os.path.exists(script_path):
                os.remove(script_path)

    def execute_query(
        self,
        query: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> List[Dict[Any, Any]]:
        try:
            df_iter = self.read_batch(asset="adhoc_query", limit=limit, offset=offset, code=query, **kwargs)
            results = []
            for df in df_iter:
                results.extend(df.to_dict(orient="records"))
                if limit and len(results) >= limit: break
            return results[:limit] if limit else results
        except Exception as e:
            raise DataTransferError(f"Script query execution failed: {e}")

    def write_batch(self, data, asset, **kwargs) -> int:
        raise NotImplementedError("Writing not supported for scripts yet.")