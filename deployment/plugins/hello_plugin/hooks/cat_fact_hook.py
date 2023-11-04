"""This module allows you to connect to the CatFactAPI."""

from airflow.hooks.base import BaseHook
import requests as re


class CatFactHook(BaseHook):
  """
  Interact with the CatFactAPI.

  Performs a connection to the CatFactAPI and retrieves a cat fact client.

  :cat_fact_conn_id: Connection ID to retrieve the CatFactAPI url.
  """

  conn_name_attr = "cat_conn_id"
  default_conn_name = "cat_conn_default"
  conn_type = "http"
  hook_name = "CatFact"

  def __init__(
      self, cat_fact_conn_id: str = default_conn_name, *args, **kwargs
  ) -> None:
    super().__init__(*args, **kwargs)
    self.cat_fact_conn_id = cat_fact_conn_id
    self.get_conn()

  def get_conn(self):
    """Function that initiates a new connection to the CatFactAPI."""

    # get the connection object from the Airflow connection
    conn = self.get_connection(self.cat_fact_conn_id)

    # return the host URL
    return conn.host

  def log_cat_facts(self, number_of_cat_facts_needed: int = 1):
    """Function that logs between 1 to 10 catfacts depending on its input."""
    if number_of_cat_facts_needed < 1:
      self.log.info(
          "You will need at least one catfact! Setting request number to 1."
      )
      number_of_cat_facts_needed = 1
    if number_of_cat_facts_needed > 10:
      self.log.info(
          f"{number_of_cat_facts_needed} are a bit many. Setting request number to 10."
      )
      number_of_cat_facts_needed = 10

    cat_fact_connection = self.get_conn()

    # log several cat facts using the connection retrieved
    for i in range(number_of_cat_facts_needed):
      cat_fact = re.get(cat_fact_connection).json()
      self.log.info(cat_fact["fact"])
    return f"{i} catfacts written to the logs!"
