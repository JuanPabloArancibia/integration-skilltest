from ast import And
import unittest
import pandas as pd
import sys
sys.path.insert(1, '/home/juanpablo/dev/backend-integration-test-master/integrations/richart_wholesale_club')
import ingestion
import requests


class testIngestion(unittest.TestCase):

  def test_process_csv_files(self):
    df = ingestion.process_csv_files()
    self.assertEqual(df.shape[1], 11)
  
  def test_clean_products(self):
    data = {'SKU': [1343, 12434, 2432],
           'EAN': [54646, 4546, 456464], 
           'BRAND_NAME': ['Samsung', 'hp', 'Presonus'],
           'ITEM_NAME': ['Led Bgh', 'Pavilion', 'Audiobox'],
           'ITEM_DESCRIPTION': ['TV 43 INCHES 1UN', 'NOTEBOOK RYZEN 7 REDEON 1UN', 'AUDIO INTERFACE 3UN'],
           'ITEM_IMG': ['https://samsung.com', 'https://hp.com', 'https://audiobox.com'],
           'CATEGORY': ['ELECTRONICS', 'ELECTRONICS', 'AUDIO'],
           'SUB_CATEGORY': ['HOME', 'WORK', 'WORK'],
           'BRANCH': ['MM', 'RHSM', 'MM'],
           'PRICE': ['172.45', '320.12', '75.10'],
           'STOCK': [4, 5, 5]}
    random_df = pd.DataFrame(data=data)
    df = ingestion.clean_products(random_df)
    for index, row in df.iterrows():
      self.assertFalse(row['ITEM_IMG'].startswith('https://')) 
      self.assertTrue(row['PACKAGE'])

  def test_clean_stock(self):
    data = {'SKU': [1343, 12434, 2432],
           'EAN': [54646, 4546, 456464], 
           'BRAND_NAME': ['Samsung', 'hp', 'Presonus'],
           'ITEM_NAME': ['Led Bgh', 'Pavilion', 'Audiobox'],
           'ITEM_DESCRIPTION': ['TV 43 INCHES 1UN', 'NOTEBOOK RYZEN 7 REDEON 1UN', 'AUDIO INTERFACE 3UN'],
           'ITEM_IMG': ['https://samsung.com', 'https://hp.com', 'https://audiobox.com'],
           'CATEGORY': ['ELECTRONICS', 'ELECTRONICS', 'AUDIO'],
           'SUB_CATEGORY': ['HOME', 'WORK', 'WORK'],
           'BRANCH': ['MM', 'RHSM', 'MM'],
           'PRICE': ['172.45', '320.12', '75.10'],
           'STOCK': [4, 5, -1]}
    
    random_df = pd.DataFrame(data=data)
    df = ingestion.clean_stock(random_df)
    for index, row in df.iterrows():
      self.assertGreater(row['STOCK'], 0)
      self.assertTrue(row['BRANCH'] == 'MM' or row['BRANCH'] == 'RHSM')
      
  def test_data_list(self):
    data = {'SKU': [1343, 12434, 2432],
           'EAN': [54646, 4546, 456464], 
           'BRAND_NAME': ['Samsung', 'hp', 'Presonus'],
           'ITEM_NAME': ['Led Bgh', 'Pavilion', 'Audiobox'],
           'ITEM_DESCRIPTION': ['TV 43 INCHES', 'NOTEBOOK RYZEN 7 REDEON', 'AUDIO INTERFACE'],
           'PACKAGE': ['1UN', '1UN', '7UN'],
           'ITEM_IMG': ['samsung.com', 'hp.com', 'audiobox.com'],
           'CATEGORY': ['ELECTRONICS', 'ELECTRONICS', 'AUDIO'],
           'SUB_CATEGORY': ['HOME', 'WORK', 'WORK'],
           'BRANCH': ['MM', 'RHSM', 'MM'],
           'PRICE': ['172.45', '320.12', '75.10'],
           'STOCK': [4, 5, 5],
          }
    
    random_df = pd.DataFrame(data=data)
    data_list = ingestion.data_list(random_df)
    for data in data_list:
      self.assertTrue(data.get('barcodes').startswith("[") and data.get('barcodes').endswith("]"))
      self.assertTrue(isinstance(data.get('sku'), str))

  def test_ingestion_api(self):
    url = "http://0.0.0.0:5000"
    tester = requests.get(url + "/ping")
    response = tester.text
    assert 'PONG' in response
    
if __name__ == '__main__':
  unittest.main()