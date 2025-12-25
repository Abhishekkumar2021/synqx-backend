from typing import Any, Dict, List, Optional, Iterator, Set
import pandas as pd
import numpy as np
import subprocess
import tempfile
import os
import sys
import json
import inspect
import random
import ast
import importlib.util
from importlib import metadata
import shutil
import io
import time
from contextlib import redirect_stdout
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
        self.timeout = config.get("timeout", 300) # Default 5 mins
        super().__init__(config)

    def validate_config(self) -> None:
        if not os.path.exists(self.base_path):
            try:
                os.makedirs(self.base_path, exist_ok=True)
            except Exception as e:
                logger.warning(f"Could not create base_path {self.base_path}: {e}")

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def _get_python_imports(self, code: str) -> Set[str]:
        """Parses Python code to find imported modules."""
        try:
            tree = ast.parse(code)
        except SyntaxError:
            return set()

        imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name.split('.')[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.add(node.module.split('.')[0])
        return imports

    def _check_python_dependencies(self, code: str) -> List[str]:
        """Checks if imported modules are available in the environment."""
        needed = self._get_python_imports(code)
        missing = []
        for module_name in needed:
            try:
                if module_name in sys.builtin_module_names:
                    continue
                spec = importlib.util.find_spec(module_name)
                if spec is None:
                    missing.append(module_name)
            except (ImportError, ValueError, AttributeError):
                pass
        return missing

    def get_environment_info(self) -> Dict[str, Any]:
        """Returns details about the execution environment."""
        installed_packages = {dist.metadata['Name']: dist.version for dist in metadata.distributions()}
        
        info = {
            "python_version": sys.version,
            "platform": sys.platform,
            "pandas_version": pd.__version__,
            "numpy_version": np.__version__,
            "base_path": self.base_path,
            "available_tools": {},
            "installed_packages": installed_packages
        }
        
        # Check standard data tools if they exist
        common_tools = [
            "jq", "curl", "wget", "aws", "gcloud", "psql", "mysql", 
            "git", "docker", "kubectl", "grep", "sed", "awk", "tar", 
            "zip", "unzip", "node", "npm", "java", "go", "python3", "pip"
        ]
        if self.config.get("required_tools"):
            common_tools.extend(self.config["required_tools"])
            
        for tool in set(common_tools):
            path = shutil.which(tool)
            if path:
                info["available_tools"][tool] = path
                
        return info

    def test_connection(self) -> bool:
        """
        Tests if the execution environment is ready.
        Checks if required dependencies for a configured script are present.
        """
        try:
            env_info = self.get_environment_info()
            logger.info(f"Custom Script Environment: Python {env_info['python_version']}, Pandas {env_info['pandas_version']}")

            # 1. Basic Python Environment Check
            local_scope = {"pd": pd, "np": np}
            exec("import pandas as pd; import numpy as np", local_scope)

            # 2. Check dependencies if code is provided in config
            if self.config.get("code") and self.config.get("language") == "python":
                missing = self._check_python_dependencies(self.config["code"])
                if missing:
                    logger.error(f"Missing Python dependencies: {', '.join(missing)}")
                    return False

            # 3. Check for shell tools if specified
            if self.config.get("required_tools"):
                for tool in self.config["required_tools"]:
                    if tool not in env_info["available_tools"]:
                        logger.error(f"Missing required shell tool: {tool}")
                        return False

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
        """Lists available script files in the base_path."""
        assets = []
        if not os.path.exists(self.base_path):
            return []
            
        for entry in os.scandir(self.base_path):
            if entry.is_file() and (entry.name.endswith(".py") or entry.name.endswith(".sh")):
                if pattern and pattern not in entry.name:
                    continue
                    
                asset_info = {
                    "name": entry.name,
                    "type": "python" if entry.name.endswith(".py") else "shell",
                    "path": entry.path
                }
                
                if include_metadata:
                    asset_info["size"] = entry.stat().st_size
                    asset_info["last_modified"] = entry.stat().st_mtime
                    
                assets.append(asset_info)
        return assets

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
        # Pre-check dependencies
        missing_deps = self._check_python_dependencies(code)
        if missing_deps:
            logger.warning(f"Script '{asset_name}' imports modules that seem missing: {missing_deps}")

        from datetime import datetime, timedelta, timezone
        local_scope = {
            "pd": pd,
            "pandas": pd,
            "json": json,
            "datetime": datetime,
            "timedelta": timedelta,
            "random": random,
            "os": os,
            "np": np,
            "numpy": np,
            "logger": logger,
        }
        
        # Capture stdout
        f = io.StringIO()
        try:
            with redirect_stdout(f):
                exec(code, local_scope)
            
            stdout_val = f.getvalue()
            if stdout_val:
                logger.info(f"Script '{asset_name}' stdout: {stdout_val.strip()}")
                
        except ImportError as e:
             raise DataTransferError(f"Missing dependency in Python script: {e}. Detected missing: {missing_deps}")
        except Exception as e:
            raise DataTransferError(f"Failed to compile/execute Python script: {e}")

        # Find the entry point
        func = local_scope.get(asset_name) or local_scope.get("extract") or local_scope.get("main")
        
        if not func or not callable(func):
            def variable_yielder():
                if "df" in local_scope and isinstance(local_scope["df"], pd.DataFrame):
                    yield local_scope["df"]
                elif "data" in local_scope and isinstance(local_scope["data"], list):
                    yield pd.DataFrame(local_scope["data"])
                else:
                    raise DataTransferError(f"No callable function ('{asset_name}', 'extract', 'main') or 'df'/'data' variable found in script.")
            
            result_iter = variable_yielder()
            filter_consumed = False
        else:
            try:
                sig = inspect.signature(func)
                call_args = {}
                if 'limit' in sig.parameters: call_args['limit'] = limit
                if 'offset' in sig.parameters: call_args['offset'] = offset
                if 'incremental_filter' in sig.parameters: call_args['incremental_filter'] = incremental_filter
                
                for k,v in kwargs.items():
                    if k in sig.parameters: call_args[k] = v

                filter_consumed = 'incremental_filter' in call_args

                result = func(**call_args)
                
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
                for col, val in incremental_filter.items():
                    if isinstance(val, (list, tuple, np.ndarray)) and len(val) > 0:
                        val = val[0]
                    elif isinstance(val, dict) and len(val) > 0:
                        val = next(iter(val.values()))

                    if col in df.columns:
                        series = df[col]
                        try:
                            if pd.api.types.is_numeric_dtype(series):
                                if isinstance(val, str):
                                    threshold = float(val.strip().replace(',', ''))
                                else:
                                    threshold = float(val)
                                df = df[series.values > threshold]
                            elif pd.api.types.is_datetime64_any_dtype(series):
                                threshold = pd.to_datetime(val)
                                if series.dt.tz is not None and threshold.tzinfo is None:
                                    threshold = threshold.replace(tzinfo=timezone.utc)
                                df = df[pd.to_datetime(series) > threshold]
                            else:
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
            start_time = time.time()
            
            while True:
                line = process.stdout.readline()
                if not line and process.poll() is not None:
                    break
                
                if time.time() - start_time > self.timeout:
                    process.kill()
                    raise DataTransferError(f"Shell script timed out after {self.timeout}s")

                if not line.strip(): continue
                try:
                    batch.append(json.loads(line))
                except json.JSONDecodeError: pass
                
                if len(batch) >= chunk_size:
                    yield pd.DataFrame(batch)
                    batch = []
            
            if batch: yield pd.DataFrame(batch)
            
            if process.returncode != 0:
                stderr = process.stderr.read()
                msg = f"Shell script failed (Exit {process.returncode}): {stderr}"
                if process.returncode == 127:
                    msg += " (Command not found - check if the required tools are installed in the environment)"
                raise DataTransferError(msg)
        except Exception as e:
            if isinstance(e, DataTransferError): raise
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