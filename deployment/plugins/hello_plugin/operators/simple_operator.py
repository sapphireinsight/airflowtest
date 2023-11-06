#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import enum
from collections import namedtuple
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Iterable, Mapping, Sequence

from typing_extensions import Literal

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake_sql_api import SnowflakeSqlApiHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

if TYPE_CHECKING:
  import pandas as pd

  from airflow.utils.context import Context


class FILE_FORMAT(enum.Enum):
  """Possible file formats."""

  CSV = enum.auto()
  JSON = enum.auto()
  PARQUET = enum.auto()


FileOptions = namedtuple("FileOptions", ["mode", "suffix", "function"])

FILE_OPTIONS_MAP = {
  FILE_FORMAT.CSV: FileOptions("r+", ".csv", "to_csv"),
  FILE_FORMAT.JSON: FileOptions("r+", ".json", "to_json"),
  FILE_FORMAT.PARQUET: FileOptions("rb+", ".parquet", "to_parquet"),
}


class simpleOperator(BaseOperator):
  def __init__(
      self,
      *,
      snowflake_conn_id: str = "snowflake_default",
      warehouse: str | None = None,
      database: str | None = None,
      role: str | None = None,
      schema: str | None = None,
      sql_query: str | None = None,
      sql_conn_id: str | None = None,
      sql_database: str | None = None,
      file_format: Literal["csv", "json", "parquet"] = "csv",
      pd_kwargs: dict | None = None,
      **kwargs,
  ) -> None:
    self.snowflake_conn_id = snowflake_conn_id,
    self.warehouse = warehouse,
    self.database = database,
    self.role = role,
    self.schema = schema,
    self.sql_query = sql_query
    self.sql_conn_id = sql_conn_id
    self.sql_database = sql_database
    self.pd_kwargs = pd_kwargs or {}

    if "path_or_buf" in self.pd_kwargs:
      raise AirflowException(
        "The argument path_or_buf is not allowed, please remove it")
  
    try:
      self.file_format = FILE_FORMAT[file_format.upper()]
    except KeyError:
      raise AirflowException(
        f"The argument file_format doesn't support {file_format} value.")
    
    super().__init__(**kwargs)

  @staticmethod
  def _fix_dtypes(df: pd.DataFrame, file_format: FILE_FORMAT) -> None:
    """
    Mutate DataFrame to set dtypes for float columns containing NaN values.
  
    Set dtype of object to str to allow for downstream transformations.
    """
    try:
      import numpy as np
      import pandas as pd
    except ImportError as e:
      from airflow.exceptions import AirflowOptionalProviderFeatureException
  
      raise AirflowOptionalProviderFeatureException(e)
  
    for col in df:
      if df[col].dtype.name == "object" and file_format == "parquet":
        # if the type wasn't identified or converted, change it to a string so if can still be
        # processed.
        df[col] = df[col].astype(str)
  
      if "float" in df[col].dtype.name and df[col].hasnans:
        # inspect values to determine if dtype of non-null values is int or float
        notna_series = df[col].dropna().values
        if np.equal(notna_series, notna_series.astype(int)).all():
          # set to dtype that retains integers and supports NaNs
          # The type ignore can be removed here if https://github.com/numpy/numpy/pull/23690
          # is merged and released as currently NumPy does not consider None as valid for x/y.
          df[col] = np.where(df[col].isnull(), None,
                             df[col])  # type: ignore[call-overload]
          df[col] = df[col].astype(pd.Int64Dtype())
        elif np.isclose(notna_series, notna_series.astype(int)).all():
          # set to float dtype that retains floats and supports NaNs
          # The type ignore can be removed here if https://github.com/numpy/numpy/pull/23690
          # is merged and released
          df[col] = np.where(df[col].isnull(), None,
                             df[col])  # type: ignore[call-overload]
          df[col] = df[col].astype(pd.Float64Dtype())

  def execute(self, context: Context) -> None:
    hook = MySqlHook(mysql_conn_id=self.sql_conn_id, schema=self.sql_database)
    data_df = hook.get_pandas_df(sql=self.sql_query)
    self.log.info("Data from SQL obtained")
  
    self._fix_dtypes(data_df, self.file_format)
    file_options = FILE_OPTIONS_MAP[self.file_format]
  
    for group_name, df in self._partition_dataframe(df=data_df):
      with NamedTemporaryFile(mode=file_options.mode,
                              suffix=file_options.suffix) as tmp_file:
        self.log.info("Writing data to temp file")
        getattr(df, file_options.function)(tmp_file.name, **self.pd_kwargs)
  
        self.log.info("Uploading data to Snnowflake")
        snowflake_hook = self._get_snowflake_hook()
        snowflake_conn = snowflake_hook.get_conn()
        snowflake_conn.cursor().execute(
            "CREATE OR REPLACE TABLE "
            "activity_type_temp_2(id integer, name string)")
        snowflake_conn.cursor().execute(
          "PUT {0} @%activity_type_temp_2".format(tmp_file.name))
        snowflake_conn.cursor().execute("COPY INTO activity_type_temp_2")
  
        self.log.info("Reading data from Snnowflake")
        for (id, name) in snowflake_conn.cursor().execute(
            "SELECT id, name FROM activity_type_temp_2"):
          print('id:{0}, name: {1}'.format(id, name))
        self.log.info("close Snnowflake connection")
        snowflake_conn.close()


  def _partition_dataframe(self, df: pd.DataFrame) -> Iterable[
    tuple[str, pd.DataFrame]]:
    """Partition dataframe using pandas groupby() method."""
    yield "", df
    # if not self.groupby_kwargs:
    #   yield "", df
    # else:
    #   grouped_df = df.groupby(**self.groupby_kwargs)
    #   for group_label in grouped_df.groups:
    #     yield group_label, grouped_df.get_group(group_label).reset_index(drop=True)


  def _get_snowflake_hook(self) -> SnowflakeSqlApiHook:
    self.log.debug("Get connection for %s", self.snowflake_conn_id)
  
    hook = SnowflakeSqlApiHook(
        snowflake_conn_id=self.snowflake_conn_id,
    )
    return hook
