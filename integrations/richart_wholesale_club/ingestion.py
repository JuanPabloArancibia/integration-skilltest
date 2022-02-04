from datetime import datetime
import os
import sched
import numpy as np
import pandas as pd
import requests
import re
import json
import time
import ast


GRAND_TYPE = os.environ.get("GRAND_TYPE")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
URL = "http://0.0.0.0:5000"

get_credentials = requests.post('{}/oauth/token?client_id={}&client_secret={}&grant_type={}'.format(URL,
                                                                                                    CLIENT_ID,
                                                                                                    CLIENT_SECRET,
                                                                                                    GRAND_TYPE)).json()

TOKEN = get_credentials.get('access_token')

def process_csv_files():
  raw_products_df = pd.read_csv("https://cornershop-scrapers-evaluation.s3.amazonaws.com/public/PRODUCTS.csv", sep="|")
  raw_price_stock_df = pd.read_csv("https://cornershop-scrapers-evaluation.s3.amazonaws.com/public/PRICES-STOCK.csv", sep="|")

  products_df = clean_products(raw_products_df)
  price_stock_df = clean_stock(raw_price_stock_df)

  dirty_df = pd.merge(products_df, price_stock_df, on='SKU')
  df = dirty_df[['SKU', 'EAN', 'BRAND_NAME', 'ITEM_NAME', 'ITEM_DESCRIPTION', 'PACKAGE', 'ITEM_IMG', 'CATEGORY', 'BRANCH', 'PRICE', 'STOCK']].copy()
  
  return df

def clean_products(products_df):
  duplicated = products_df.duplicated(subset=["SKU"])
  if duplicated.any():
    products_df.drop_duplicates(subset=["SKU"], keep='first', inplace=True)
  
  products_df = products_df.replace({'https://': '', '<p>': '', "</p>": ''}, regex=True)
  products_df['CATEGORY'] =  products_df['CATEGORY'] + "|" + products_df["SUB_CATEGORY"]

  desc_df = products_df["ITEM_DESCRIPTION"].str.replace('.', '')
  products_df["PACKAGE"] = desc_df.apply(lambda x: re.findall(r'((?!0)\d+\s*\D+$)', x))
  products_df['PACKAGE'] = products_df['PACKAGE'].apply(''.join)

  return products_df

def clean_stock(price_stock_df):
  price_stock_df = price_stock_df[price_stock_df['STOCK'] > 0]
  price_stock_df = price_stock_df[(price_stock_df['BRANCH'] == 'MM') | 
                                  (price_stock_df['BRANCH'] == 'RHSM')]

  return price_stock_df
  
def data_list(df):
  header = {'token': 'Bearer {}'.format(TOKEN)}
  merchants = requests.get("{}/api/merchants".format(URL), headers=header).json()
  richards = list(filter(lambda merchant: merchant['name'] == "Richard's", merchants.get('merchants')))[0] #Use next instead of list
  richards_df = df.sort_values(['PRICE'], ascending=False).groupby('BRAND_NAME').head(100)
  richards_df.insert(0, "merchant_id", richards['id'])
  richards_df.insert(1, "url", '') #this column is not present in any csv file
  richards_df = richards_df.rename(columns={'SKU': 'sku', 'EAN': 'barcodes', 'BRAND_NAME': 'brand', 'ITEM_NAME': 'name',
                                            'ITEM_DESCRIPTION': 'description', 'PACKAGE': 'package', 'ITEM_IMG': 'image_url',
                                            'CATEGORY': 'category', 'BRANCH': 'branch', 'PRICE': 'price', 'STOCK': 'stock'})
                   
  richards_df[['sku', 'barcodes']] = richards_df[['sku', 'barcodes']].astype(str)
  richards_df['barcodes'] = "['" + richards_df['barcodes'] + "']"
  products_json = (richards_df.groupby(['merchant_id', 'sku', 'barcodes', 'brand', 'name', 'description', 'package', 'image_url', 'category', 'url'])
                    .apply(lambda x: x[['branch', 'stock', 'price']].to_dict('records'))
                    .reset_index()
                    .rename(columns={0:'branch_products'})
                    .to_json(orient='records'))

  data_list = json.loads(products_json)
  
  return data_list


def post_products(url, header, data, session): 
  data['barcodes'] = ast.literal_eval(data['barcodes'])
  add_products = session.post("{}/api/products".format(url), headers=header, json=data)

def ingestion_api(data_list):
  header = {'token': 'Bearer {}'.format(TOKEN)}
  merchants = requests.get("{}/api/merchants".format(URL), headers=header).json()
  richards = list(filter(lambda merchant: merchant['name'] == "Richard's", merchants.get('merchants')))[0] #Use next instead of list
  beauty = list(filter(lambda merchant: merchant['name'] == "Beauty", merchants.get('merchants')))[0] #Use next instead of list
  active = {'id': richards['id'], 'name': 'test', 'is_active': True, 'can_be_updated': True, 'can_be_deleted': False}
  is_active = requests.put("{}/api/merchants/{}".format(URL, richards['id']), headers=header, json=active)
  delete = requests.delete("{}/api/merchants/{}".format(URL, beauty['id']), headers=header)

  s = sched.scheduler(time.time, time.sleep)
  session = requests.Session() #todo: It should be inside post_products method
  counter = 0
  for data in data_list:
    print('sending products...')   
    if counter == 9999:
      dt = datetime.datetime.now()
      tomorrow = dt + datetime.timedelta(days=1)
      seconds = int((datetime.datetime.combine(tomorrow, datetime.time.min) - dt).total_seconds())
      print("Wait" + seconds + ",limit of request per day")    
      s.enter(seconds, 1, post_products, kwargs={'url': URL, 'header': header, 'data': data, 'session': session})
      s.run()
      counter = 0
    else:
      s.enter(2, 1, post_products, kwargs={'url': URL, 'header': header, 'data': data, 'session': session})
      s.run()
      counter += 1

if __name__ == "__main__":
  df = process_csv_files()
  data_list = data_list(df)
  ingestion_api(data_list)
