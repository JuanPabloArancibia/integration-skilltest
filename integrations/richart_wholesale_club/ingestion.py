import os
import numpy as np
import pandas as pd
import requests
import re
import json


GRAND_TYPE = os.environ.get("GRAND_TYPE")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
URL = "https://peaceful-inlet-73527.herokuapp.com/"
#pong = requests.get("https://peaceful-inlet-73527.herokuapp.com/ping")

get_credentials = requests.post('{}/oauth/token?client_id={}&client_secret={}&grant_type={}'.format(URL,
                                                                                                    CLIENT_ID,
                                                                                                    CLIENT_SECRET,
                                                                                                    GRAND_TYPE)).json()
TOKEN = get_credentials["access_token"]


def process_csv_files():
  raw_products_df = pd.read_csv("https://cornershop-scrapers-evaluation.s3.amazonaws.com/public/PRODUCTS.csv", sep="|")
  raw_price_stock_df = pd.read_csv("https://cornershop-scrapers-evaluation.s3.amazonaws.com/public/PRICES-STOCK.csv", sep="|")

  products_df = clean_products(raw_products_df)
  price_stock_df = clean_stock(raw_price_stock_df)

  df = pd.merge(products_df, price_stock_df, on='SKU')

  ingestion_api(df)
  return print("PRueba")

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
  
def ingestion_api(df):
  header = {'token': 'Bearer {}'.format(TOKEN)}
  merchants = requests.get("{}/api/merchants".format(URL), headers=header).json()
  richards = list(filter(lambda merchant: merchant['name'] == "Richard's", merchants['merchants']))[0]
  beauty = list(filter(lambda merchant: merchant['name'] == "Beauty", merchants['merchants']))[0]

  is_active = requests.put("{}/api/merchants/{}".format(URL, richards['id']), headers=header, data=json.loads(json.dumps({'is_active': True})))
  delete = requests.delete("{}/api/merchants/{}".format(URL, beauty['id']), headers=header)

  richards_df = df.sort_values(['PRICE'], ascending=False).groupby('BRAND_NAME').head(100)

  richards_df.insert(0, "merchant_id", richards['id'])
  richards_df.insert(1, "url", '') #this column is not present in any csv file
  richards_df = richards_df.rename(columns={'SKU': 'sku', 'EAN': 'barcodes', 'BRAND_NAME': 'brand', 'ITEM_NAME': 'name',
                                            'ITEM_DESCRIPTION': 'description', 'PACKAGE': 'package', 'ITEM_IMG': 'image_url',
                                            'CATEGORY': 'category', 'BRANCH': 'branch', 'PRICE': 'price', 'STOCK': 'stock'})
  #richards_df = richards_df.head()
  products_json = (richards_df.groupby(['merchant_id', 'sku', 'barcodes', 'brand', 'name', 'description', 'package', 'image_url', 'category', 'url'])
                    .apply(lambda x: x[['branch', 'stock', 'price']].to_dict('records'))
                    .reset_index()
                    .rename(columns={0:'branch_products'})
                    .to_json(orient='records'))
  
  add_products = requests.post("{}/api/products".format(URL), json= json.loads(products_json))

    
if __name__ == "__main__":
  process_csv_files()
