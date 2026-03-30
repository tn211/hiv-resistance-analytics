#!/usr/bin/env python
# coding: utf-8

# In[19]:


import duckdb, boto3, os
os.makedirs('data', exist_ok=True)


# In[11]:


conn = duckdb.connect('hivdb_resistance.duckdb')


# In[12]:


session = boto3.Session(profile_name='hiv-project')
s3 = session.client('s3')
bucket_name = 'hiv-data-022784797781'


# In[13]:


tables = ['pi', 'nrti', 'nnrti', 'ini', 'cai']


# In[14]:


for table in tables:
    local_path = f"data/Genotype-Phenotype-{table}.parquet"
    conn.execute(
        f"COPY hivdb.{table} TO '{local_path}' (FORMAT PARQUET)"
    )
    s3_key = f"datasets/{table}-Genotype-Phenotype/data.parquet"
    s3.upload_file(local_path, bucket_name, s3_key)
    print(f"Uploaded {table} -> s3://{bucket_name}/{s3_key}")


# In[17]:


conn.close()

