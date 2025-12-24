from typing import Any, Dict, List, Optional, Iterator, Union
import pandas as pd
import subprocess
import tempfile
import os
import json
import inspect
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
        return True

    def discover_assets(
        self, pattern: Optional[str] = None, include_metadata: bool = False, **kwargs
    ) -> List[Dict[str, Any]]:
        # This connector is "Asset-First", meaning assets are usually defined manually.
        # However, we could scan the base_path for .py or .sh files?
        # For now, return empty as discovery is not primary mode.
        return []

    def infer_schema(
        self, asset: str, sample_size: int = 1000, **kwargs
    ) -> Dict[str, Any]:
        # We need to run the script to infer schema.
        try:
            df = next(self.read_batch(asset, limit=10, **kwargs))
            columns = [{"name": col, "type": str(dtype)} for col, dtype in df.dtypes.items()]
            return {"asset": asset, "columns": columns}
        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to infer schema for script '{asset}': {e}")

    def read_batch(
        self, asset: str, limit: Optional[int] = None, offset: Optional[int] = None, **kwargs
    ) -> Iterator[pd.DataFrame]:
        
        code = kwargs.get("code") or kwargs.get("query") # Support 'query' as alias for code
        language = kwargs.get("language", "python")
        
        if not code:
            # Maybe it's a file on disk?
            # Check if asset matches a file in base_path
            possible_path = os.path.join(self.base_path, asset)
            if os.path.exists(possible_path):
                if possible_path.endswith(".py"):
                    language = "python"
                    with open(possible_path, "r") as f: code = f.read()
                elif possible_path.endswith(".sh"):
                    language = "shell"
                    code = possible_path # For shell, we can run the file directly
            else:
                raise DataTransferError(f"No code provided for asset '{asset}' and file not found.")

        if language == "python":
            yield from self._execute_python(asset, code, limit, offset, **kwargs)
        elif language == "shell":
            yield from self._execute_shell(asset, code, limit, offset, **kwargs)
        else:
            raise ConfigurationError(f"Unsupported script language: {language}")

    def _execute_python(self, asset_name: str, code: str, limit: int, offset: int, **kwargs) -> Iterator[pd.DataFrame]:
        """
        Executes Python code in-process. 
        Expects the code to define a function matching `asset_name` OR `extract` OR `main`.
        """
        local_scope = {
            "pd": pd,
            "pandas": pd,
            "json": json
        }
        
        try:
            exec(code, local_scope)
        except Exception as e:
            raise DataTransferError(f"Failed to compile/execute Python script: {e}")

        # Find the entry point
        func = local_scope.get(asset_name) or local_scope.get("extract") or local_scope.get("main")
        
        if not func or not callable(func):
            # Fallback: Did the script produce a variable named 'data' or 'df'?
            if "df" in local_scope and isinstance(local_scope["df"], pd.DataFrame):
                yield local_scope["df"]
                return
            if "data" in local_scope and isinstance(local_scope["data"], list):
                yield pd.DataFrame(local_scope["data"])
                return
            
            raise DataTransferError(f"No callable function ('{asset_name}', 'extract', 'main') or 'df'/'data' variable found in script.")

        # Call the function
        try:
            # Inspect signature to pass args if accepted
            sig = inspect.signature(func)
            call_args = {}
            if 'limit' in sig.parameters: call_args['limit'] = limit
            if 'offset' in sig.parameters: call_args['offset'] = offset
            
            # Pass kwargs too if they match
            for k,v in kwargs.items():
                if k in sig.parameters: call_args[k] = v

            result = func(**call_args)
            
            if isinstance(result, pd.DataFrame):
                yield result
            elif isinstance(result, list):
                yield pd.DataFrame(result)
            elif isinstance(result, Iterator):
                for chunk in result:
                    if isinstance(chunk, pd.DataFrame): yield chunk
                    elif isinstance(chunk, list): yield pd.DataFrame(chunk)
                    else: yield pd.DataFrame([chunk])
            else:
                yield pd.DataFrame([result])

        except Exception as e:
            raise DataTransferError(f"Error during Python execution: {e}")

    def _execute_shell(self, asset_name: str, code: str, limit: int, offset: int, **kwargs) -> Iterator[pd.DataFrame]:
        """
        Executes a shell command. 
        If 'code' looks like a file path, runs it.
        Otherwise, writes to temp file and runs.
        Expects stdout to be JSON Lines or CSV.
        """
        is_file = os.path.exists(code) and os.path.isfile(code)
        
        cmd_env = os.environ.copy()
        cmd_env.update(self.env_vars)
        
        # Pass limit/offset as env vars
        if limit: cmd_env['LIMIT'] = str(limit)
        if offset: cmd_env['OFFSET'] = str(offset)

        if is_file:
            script_path = code
            cmd = ["bash", script_path]
        else:
            # Write to temp file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as tf:
                if not code.startswith("#!"):
                    tf.write("#!/bin/bash\n")
                tf.write(code)
                script_path = tf.name
            
            # Make executable
            os.chmod(script_path, 0o755)
            cmd = ["bash", script_path]

        try:
            process = subprocess.Popen(
                cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, 
                env=cmd_env, 
                text=True
            )
            
            # Read line by line and yield chunks
            # Assuming JSONL for now
            batch = []
            chunk_size = 5000
            
            for line in process.stdout:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    batch.append(data)
                except json.JSONDecodeError:
                    # Maybe it's not JSON? Log warning?
                    # Or maybe it's CSV? Detection is hard on stream.
                    # For now, strict JSONL or we fail/skip.
                    pass
                
                if len(batch) >= chunk_size:
                    yield pd.DataFrame(batch)
                    batch = []
            
            if batch:
                yield pd.DataFrame(batch)
                
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
    ) -> List[Dict[str, Any]]:
        """
        Treats the query string as the script code to execute.
        """
        try:
            # We use read_batch but pass the query as the 'code' kwarg
            # We assume python language by default for ad-hoc queries if not specified
            df_iter = self.read_batch(
                asset="adhoc_query", 
                limit=limit, 
                offset=offset, 
                code=query, 
                **kwargs
            )
            
            results = []
            for df in df_iter:
                results.extend(df.to_dict(orient="records"))
                if limit and len(results) >= limit:
                    break
            
            return results[:limit] if limit else results
        except Exception as e:
            raise DataTransferError(f"Script query execution failed: {e}")

    def write_batch(self, data, asset, **kwargs) -> int:
        raise NotImplementedError("Writing not supported for scripts yet.")
