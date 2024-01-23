import pandas
import requests
import time
from typing import List, Dict
import pandas as pd

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.pandas_tools import write_pandas

from datetime import datetime, timedelta
import concurrent.futures
import os

bearer = "Bearer " + os.environ['BEARER_TOKEN']

BASE_URL = "https://api.github.com"
COMMITS_ENDPOINT = "/repos/{owner}/{repo}/commits"
HEADERS = {"Accept": "application/vnd.github.v3+json", "Authorization": bearer}
PAGE_SIZE = 100

SNOWFLAKE_USER = os.environ['SNOWFLAKE_USER']
SNOWFLAKE_PASS = os.environ['SNOWFLAKE_PASS']
SNOWFLAKE_ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
SNOWFLAKE_WAREHOUSE = os.environ['SNOWFLAKE_WAREHOUSE']


def wait_for_rate_limit(headers: dict) -> None:
  """
  Waits until the rate limit resets.

  Args:
  headers: The headers from the last response.
  """
  reset_time = int(headers.get('X-RateLimit-Reset', time.time()))
  sleep_time = max(0, reset_time - int(time.time()))
  time.sleep(sleep_time)


def fetch_commit_history(owner: str, repo: str) -> List[Dict]:
  """
  Fetches the commit history from a GitHub repository.

  Args:
  owner: The owner of the repository.
  repo: The name of the repository.

  Returns:
  A list of commit dictionaries.
  """
  url = BASE_URL + COMMITS_ENDPOINT.format(owner=owner, repo=repo)
  params = {"per_page": PAGE_SIZE}
  page = 1
  commit_history = []

  # Get the date three days ago
  three_days_ago = datetime.now() - timedelta(days=3)
  # Format the date in ISO 8601 format
  since_date = three_days_ago.isoformat()

  while True:
    response = requests.get(url,
                            headers=HEADERS,
                            params={
                                **params, "page": page,
                                "since": since_date
                            })

    if response.status_code == 403:
      wait_for_rate_limit(response.headers)
      time.sleep(1)  # Add a delay here
      continue

    if response.status_code != 200:
      raise Exception(f"Failed to fetch commit history: {response.content}")

    data = response.json()
    commit_history.extend([{
        "date": commit["commit"]["author"]["date"],
        "username": commit["commit"]["author"]["name"],
        "repo_name": f"{owner}/{repo}"
    } for commit in data])

    if "next" in response.links:
      page += 1
    else:
      break

  return commit_history


def execute_sql(sql_string, **kwargs):
  conn = snowflake.connector.connect(user=SNOWFLAKE_USER,
                                     password=SNOWFLAKE_PASS,
                                     account=SNOWFLAKE_ACCOUNT,
                                     warehouse=SNOWFLAKE_WAREHOUSE,
                                     database="SCROLLSTATS",
                                     schema="DBT_SCROLLSTATS")

  sql = sql_string.format()
  res = conn.cursor(DictCursor).execute(sql)
  results = res.fetchall()
  conn.close()
  return results


def increment_table(df, **kwargs):
  df.columns = df.columns.str.upper()

  conn = snowflake.connector.connect(user=SNOWFLAKE_USER,
                                     password=SNOWFLAKE_PASS,
                                     account=SNOWFLAKE_ACCOUNT,
                                     warehouse=SNOWFLAKE_WAREHOUSE,
                                     database="SCROLLSTATS",
                                     schema="DBT_SCROLLSTATS")

  table_name = "SCROLLSTATS_GIT_COMMITS"
  temp_table_name = "TEMP_" + table_name

  cursor = conn.cursor()

  # Create a temporary table with the same structure as the main table
  create_temp_table_query = f"""
  CREATE TEMPORARY TABLE {temp_table_name} (
      DATE TIMESTAMP_NTZ(9),
      USERNAME VARCHAR(255),
      REPO_NAME VARCHAR(255)
  );
  """
  cursor.execute(create_temp_table_query)

  # Write the DataFrame to the temporary table
  success, nchunks, nrows, _ = write_pandas(conn, df, temp_table_name)

  # Merge the temporary table with the main table
  merge_query = f"""
  INSERT INTO {table_name}
  SELECT * FROM {temp_table_name}
  WHERE NOT EXISTS (
      SELECT 1 FROM {table_name}
      WHERE {table_name}.DATE = {temp_table_name}.DATE
      AND {table_name}.USERNAME = {temp_table_name}.USERNAME
      AND {table_name}.REPO_NAME = {temp_table_name}.REPO_NAME
  );
  """
  cursor.execute(merge_query)

  cursor.close()
  conn.close()

  # Check if the operation was successful
  if success:
    print(
        f"DataFrame successfully written to {table_name}. Rows inserted: {nrows}"
    )
  else:
    print("Failed to write DataFrame to Snowflake.")


repos = execute_sql('''
SELECT * FROM SCROLLSTATS.DBT_SCROLLSTATS.SCROLLSTATS_LABELS_REPOS
''')

# Initialize an empty list to hold the dataframes
dfs = []


def fetch_all_commit_histories(repos):
  with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = {
        executor.submit(fetch_commit_history, repo['OWNER'], repo['NAME']):
        repo
        for repo in repos
    }
    for future in concurrent.futures.as_completed(futures):
      repo = futures[future]
      url = repo['URL']
      try:
        data = future.result()
        df = pd.DataFrame(data)
        dfs.append(df)
        print(f'{url} Completed Successfully!')
      except Exception as exc:
        print(f'{url} Generated an exception: {exc}')


fetch_all_commit_histories(repos)

#concat all dfs in the list into one final df
df = pd.concat(dfs, ignore_index=True)

increment_table(df)
