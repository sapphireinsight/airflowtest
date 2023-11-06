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
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake_sql_api import SnowflakeSqlApiHook



if TYPE_CHECKING:
  import pandas as pd

  from airflow.providers.common.sql.hooks.sql import DbApiHook
  from airflow.utils.context import Context


[docs]class FILE_FORMAT(enum.Enum):
  """Possible file formats."""

[docs]    CSV = enum.auto()
[docs]    JSON = enum.auto()
[docs]    PARQUET = enum.auto()


[docs]FileOptions = namedtuple("FileOptions", ["mode", "suffix", "function"])

[docs]FILE_OPTIONS_MAP = {
  FILE_FORMAT.CSV: FileOptions("r+", ".csv", "to_csv"),
  FILE_FORMAT.JSON: FileOptions("r+", ".json", "to_json"),
  FILE_FORMAT.PARQUET: FileOptions("rb+", ".parquet", "to_parquet"),
}


[docs]class SqlToS3Operator(BaseOperator):
  """
  Saves data from a specific SQL query into a file in S3.

  .. seealso::
      For more information on how to use this operator, take a look at the guide:
      :ref:`howto/operator:SqlToS3Operator`

  :param query: the sql query to be executed. If you want to execute a file, place the absolute path of it,
      ending with .sql extension. (templated)
  :param s3_bucket: bucket where the data will be stored. (templated)
  :param s3_key: desired key for the file. It includes the name of the file. (templated)
  :param replace: whether or not to replace the file in S3 if it previously existed
  :param sql_conn_id: reference to a specific database.
  :param sql_hook_params: Extra config params to be passed to the underlying hook.
      Should match the desired hook constructor params.
  :param parameters: (optional) the parameters to render the SQL query with.
  :param aws_conn_id: reference to a specific S3 connection
  :param verify: Whether or not to verify SSL certificates for S3 connection.
      By default SSL certificates are verified.
      You can provide the following values:

      - ``False``: do not validate SSL certificates. SSL will still be used
              (unless use_ssl is False), but SSL certificates will not be verified.
      - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
              You can specify this argument if you want to use a different
              CA cert bundle than the one used by botocore.
  :param file_format: the destination file format, only string 'csv', 'json' or 'parquet' is accepted.
  :param pd_kwargs: arguments to include in DataFrame ``.to_parquet()``, ``.to_json()`` or ``.to_csv()``.
  :param groupby_kwargs: argument to include in DataFrame ``groupby()``.
  """

[docs]    template_fields: Sequence[str] = (
  "s3_bucket",
  "s3_key",
  "query",
  "sql_conn_id",
)
[docs]    template_ext: Sequence[str] = (".sql",)
[docs]    template_fields_renderers = {
  "query": "sql",
  "pd_kwargs": "json",
}

def __init__(
    self,
    *,
    snowflake_conn_id: str = "snowflake_default",
    warehouse: str | None = None,
    database: str | None = None,
    role: str | None = None,
    schema: str | None = None,
    # s3_bucket: str,
    # s3_key: str,
    query: str,
    sql_conn_id: str,
    sql_hook_params: dict | None = None,
    parameters: None | Mapping | Iterable = None,
    replace: bool = False,
    # aws_conn_id: str = "aws_default",
    # verify: bool | str | None = None,
    file_format: Literal["csv", "json", "parquet"] = "csv",
    pd_kwargs: dict | None = None,
    groupby_kwargs: dict | None = None,
    **kwargs,
) -> None:
  super().__init__(**kwargs)
  self.snowflake_conn_id =snowflake_conn_id,
  self.warehouse = warehouse,
  self.database = database,
  self.role = role,
  self.schema = schema,
  self.query = query
  # self.s3_bucket = s3_bucket
  # self.s3_key = s3_key
  self.sql_conn_id = sql_conn_id
  # self.aws_conn_id = aws_conn_id
  # self.verify = verify
  self.replace = replace
  self.pd_kwargs = pd_kwargs or {}
  self.parameters = parameters
  self.groupby_kwargs = groupby_kwargs or {}
  self.sql_hook_params = sql_hook_params

  if "path_or_buf" in self.pd_kwargs:
    raise AirflowException("The argument path_or_buf is not allowed, please remove it")

  try:
    self.file_format = FILE_FORMAT[file_format.upper()]
  except KeyError:
    raise AirflowException(f"The argument file_format doesn't support {file_format} value.")

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
        df[col] = np.where(df[col].isnull(), None, df[col])  # type: ignore[call-overload]
        df[col] = df[col].astype(pd.Int64Dtype())
      elif np.isclose(notna_series, notna_series.astype(int)).all():
        # set to float dtype that retains floats and supports NaNs
        # The type ignore can be removed here if https://github.com/numpy/numpy/pull/23690
        # is merged and released
        df[col] = np.where(df[col].isnull(), None, df[col])  # type: ignore[call-overload]
        df[col] = df[col].astype(pd.Float64Dtype())

[docs]    def execute(self, context: Context) -> None:
  sql_hook = self._get_hook()
  # s3_conn = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
  # snowflake_conn = SnowflakeSqlApiHook(snowflake_conn_id=self.snowflake_conn_id,  sql=SQL_INSERT_STATEMENT, database=SNOWFLAKE_DATABASE, warehouse=SNOWFLAKE_WAREHOUSE, schema=SNOWFLAKE_SCHEMA,role=SNOWFLAKE_ROLE)
  data_df = sql_hook.get_pandas_df(sql=self.query, parameters=self.parameters)
  self.log.info("Data from SQL obtained")

  self._fix_dtypes(data_df, self.file_format)
  file_options = FILE_OPTIONS_MAP[self.file_format]

  for group_name, df in self._partition_dataframe(df=data_df):
    with NamedTemporaryFile(mode=file_options.mode, suffix=file_options.suffix) as tmp_file:
      self.log.info("Writing data to temp file")
      getattr(df, file_options.function)(tmp_file.name, **self.pd_kwargs)

      # self.log.info("Uploading data to S3")
      # object_key = f"{self.s3_key}_{group_name}" if group_name else self.s3_key
      # s3_conn.load_file(
      #     filename=tmp_file.name, key=object_key, bucket_name=self.s3_bucket, replace=self.replace
      # )

      self.log.info("Uploading data to Snnowflake")
      snowflake_hook = _get_snowflake_hook()
      snowflake_conn = snowflake_hook.get_conn()
      snowflake_conn.cursor().execute(
          "CREATE OR REPLACE TABLE "
          "activity_type_temp_2(id integer, name string)")
      snowflake_conn.cursor().execute("PUT file:///tmp/data/file* @%activity_type_temp_2")
      snowflake_conn.cursor().execute("COPY INTO activity_type_temp_2")

      self.log.info("Reading data from Snnowflake")
      for (id, name) in snowflake_conn.cursor().execute("SELECT id, name FROM activity_type_temp_2"):
        print('id:{0}, name: {1}'.format(id, name))
      self.log.info("close Snnowflake connection")
      snowflake_conn.close()


def _partition_dataframe(self, df: pd.DataFrame) -> Iterable[tuple[str, pd.DataFrame]]:
  """Partition dataframe using pandas groupby() method."""
  yield "", df
  # if not self.groupby_kwargs:
  #   yield "", df
  # else:
  #   grouped_df = df.groupby(**self.groupby_kwargs)
  #   for group_label in grouped_df.groups:
  #     yield group_label, grouped_df.get_group(group_label).reset_index(drop=True)

def _get_hook(self) -> DbApiHook:
  self.log.debug("Get connection for %s", self.sql_conn_id)
  conn = BaseHook.get_connection(self.sql_conn_id)
  hook = conn.get_hook(hook_params=self.sql_hook_params)
  if not callable(getattr(hook, "get_pandas_df", None)):
    raise AirflowException(
        "This hook is not supported. The hook class must have get_pandas_df method."
    )
  return hook


def _get_snowflake_hook(self) -> SnowflakeSqlApiHook:
  self.log.debug("Get connection for %s", self.snowflake_conn_id)

  hook = SnowflakeSqlApiHook(
      snowflake_conn_id=self.snowflake_conn_id,
      database=self.database,
      warehouse=self.warehouse,
      schema=self.schema,
      role=self.role
  )
  return hook



# from snowflake.snowpark.session import Session, FileOperation
# import os
#
# file="test.csv"
# csv_file="\\\\Sharedpath\\share"
# archive_file="\\\\Sharedpath\\archive"
#
# # Create Session object
# def create_session_object():
#   connection_parameters = {
#     "account": "XXX",
#     "user": "XXXX",
#     "password": "XXX",
#     "role": "ACCOUNTADMIN",
#     "warehouse": "COMPUTE_WH",
#     "database": "EXERCISE_DB",
#     "schema": "EXCERCISE_SCHEMA"
#   }
#   session = Session.builder.configs(connection_parameters).create()
#   return session
#
# def load_data(session):
#   # Create internal stage if it does not exists
#   session.sql("create or replace stage demo ").collect()
#
#   #Upload file to stage
#   FileOperation(session).put(os.path.join(csv_file,file), '@demo/test.csv')
#
#   #create or replace snowflake table
#   session.sql("create or replace table demo_csv(ID INT, first_name varchar)").collect()
#
#   #load table from stage
#   session.sql("copy into demo_csv from @demo file_format= (type = csv field_delimiter=',' skip_header=1)").collect()
#
#   #drop stage
#   session.sql("drop stage demo").collect()
#
#   #Move/Archive/rename file to other folder
#   os.rename(os.path.join(csv_file,file),os.path.join(archive_file,file))
