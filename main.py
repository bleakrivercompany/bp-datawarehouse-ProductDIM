# -*- coding: utf-8 -*-
# main
import requests
import pandas as pd
import math
from flatten_json import flatten
import numpy as np
from datetime import date
from datetime import datetime
from google.cloud import storage
from google.cloud import secretmanager
from google.api_core import exceptions
from bp_utils import get_bp_secret
import json
from google.oauth2 import service_account

# Define project
project_id = f"button-datawarehouse"
# Define storage bucket for push
bucket_name = "cs-royalties-test"  # Replace with your bucket name
# Define secrets to fetch
p_consumerkey = get_bp_secret(project_id, "wc_consumer_key", "latest")
p_consumersecret = get_bp_secret(project_id, "wc_consumer_secret", "latest")
secret_id_for_sa_key = "storage_sa_key" # The secret you just created
# get those secrets
sa_key_json_string = get_bp_secret(project_id, secret_id_for_sa_key)
credentials_info = json.loads(sa_key_json_string)
credentials = service_account.Credentials.from_service_account_info(credentials_info)
storage_client = storage.Client(credentials=credentials, project=project_id)

# Start the API Call
pg = ('https://buttonpoetry.com/wp-json/wc/v3/products?consumer_key=' + p_consumerkey + '&consumer_secret=' + p_consumersecret)

pg_max = requests.get(pg)
print(pg_max.headers)
total_prods = pg_max.headers.get('x-wp-total')

page_max = int(total_prods)

page_max = math.ceil(page_max/10)

#print(page_max)

Products_Run = pd.DataFrame(columns= ['id'])

page_ct = 0

while page_ct < page_max:
    page_ct += 1
    # Using an f-string for cleaner URL building
    url = (
        f"https://buttonpoetry.com/wp-json/wc/v3/products?"
        f"consumer_key={p_consumerkey}&"
        f"consumer_secret={p_consumersecret}&"
        f"page={page_ct}&"
        f"per_page=10"
    )
    print(url)
    # It's good practice to use a different variable name for the response
    response_json = requests.get(url).json()
    
    url_flat = [flatten(d) for d in response_json]
    flat2 = pd.DataFrame(url_flat)
    Products_Run = pd.concat([Products_Run, flat2], ignore_index=True)

# Upload Full Product Query
destination_blob_name = "stage/dim_stage/Product_Full_WC_Query.csv"  # Desired filename in GCS
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
# Upload the DataFrame as a CSV string
blob.upload_from_string(Products_Run.to_csv(index=False), content_type="text/csv")

#Cleanup for Final dimProduct
Products_Dim = Products_Run[['id', 'name', 'type', 'categories_0_id', 'categories_0_name', 'shipping_class' ]]

Products_Dim['name'] = Products_Dim['name'].astype(str).map(lambda x: x.strip())
Products_Dim['name'] = Products_Dim['name'].astype(str).map(lambda x: x.replace(u'\u201c', ''))
Products_Dim['name'] = Products_Dim['name'].astype(str).map(lambda x: x.replace(u'\u201d', ''))
Products_Dim['name'] = Products_Dim['name'].astype(str).map(lambda x: x.replace(u'&ndash; ', ''))
Products_Dim['name'] = Products_Dim['name'].astype(str).map(lambda x: x.replace(u' <BR>&nbsp;<BR>', ''))
Products_Dim['name'] = Products_Dim['name'].astype(str).map(lambda x: x.replace(u'#038; ', ''))

conditions = [
    #Books
    (Products_Dim['categories_0_name'] == 'Books')  ,
    (Products_Dim['categories_0_name'] == 'Forthcoming Books'),
    (Products_Dim['categories_0_name'] == 'Out of Print'),
    (Products_Dim['categories_0_name'] == 'Audiobooks'),
    (Products_Dim['categories_0_name'] == 'E-Books') ,
    (Products_Dim['shipping_class'] == 'books'),
    #Bundles
    (Products_Dim['categories_0_name'] == 'Bundles')  ,
    (Products_Dim['name'].str.contains(r'Bundle')),
    (Products_Dim['shipping_class'] == 'bundles'),
    #Merch
    (Products_Dim['categories_0_name'] == 'Merch'),
    (Products_Dim['categories_0_name'] == 'Featured') ,
    (Products_Dim['shipping_class'] == 'clothing'),
    #Other
    (Products_Dim['categories_0_name'] == 'Workshop')
    ]
choices = ['Book', 'Book', 'Book', 'Book', 'Book', 'Book', 'Bundles', 'Bundles', 'Bundles', 'Merch', 'Merch', 'Merch', 'Workshop' ]

Products_Dim['Product_Category'] = np.select(conditions, choices, default = 'Check')

Products_Dim = Products_Dim.drop(columns=['categories_0_name', 'categories_0_id'])

# Upload Full Product Query
destination_blob_name = "dimension_tables/Product_Dim.csv"  # Desired filename in GCS
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
# Upload the DataFrame as a CSV string
blob.upload_from_string(Products_Run.to_csv(index=False), content_type="text/csv")

