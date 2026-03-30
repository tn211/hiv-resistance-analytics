#!/usr/bin/env python
# coding: utf-8

# In[1]:


import dlt, duckdb, csv, requests
from io import StringIO


# In[2]:


HIVDB_DATASETS = {
    "pi": "https://hivdb.stanford.edu/download/GenoPhenoDatasets/PI_DataSet.txt",
    "nrti": "https://hivdb.stanford.edu/download/GenoPhenoDatasets/NRTI_DataSet.txt",
    "nnrti": "https://hivdb.stanford.edu/download/GenoPhenoDatasets/NNRTI_DataSet.txt",
    "ini": "https://hivdb.stanford.edu/download/GenoPhenoDatasets/INI_DataSet.txt",
    "cai": "https://hivdb.stanford.edu/download/GenoPhenoDatasets/CAI_DataSet.txt"
}


# In[3]:


@dlt.source
def hivdb_source(username, password):
    session = requests.Session()
    session.auth = (username, password)

    for name, url in HIVDB_DATASETS.items():

        @dlt.resource(name=name, write_disposition="replace")
        def fetch_dataset(_url=url, _name=name):
            response = session.get(_url)
            response.raise_for_status()
            reader = csv.DictReader(StringIO(response.text), delimiter="\t")
            for row in reader:
                yield row

        yield fetch_dataset


pipeline = dlt.pipeline(
    pipeline_name="hivdb_resistance",
    destination="duckdb",
    dataset_name="hivdb",
)

load_info = pipeline.run(hivdb_source(
    username="your_username",
    password="your_password",
))
print(load_info)


# In[4]:


import os
duckdb_path = os.path.join(pipeline.working_dir, 'duckdb', 'hivdb_resistance.duckdb')
print(f"Path: {duckdb_path}")
print(f"Exists: {os.path.exists(duckdb_path)}")


# In[5]:


import duckdb
conn = duckdb.connect('hivdb_resistance.duckdb')
print('Tables:', conn.execute('SHOW TABLES').fetchall())
print('Schemas:', conn.execute('SELECT schema_name FROM information_schema.schemata').fetchall())


# In[6]:


print(conn.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall())


# In[7]:


conn.close()

