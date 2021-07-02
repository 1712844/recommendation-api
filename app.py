from rapidfuzz import fuzz, process
import pyodbc
import logging
import sys
import pandas as pd
from flask import Flask, request, make_response, jsonify, Response

server = 'doantuvansanpham.database.windows.net'
database = 'NDS_Final'
username = 'doantuvansanpham'
password = 'k17hcmus!'   
driver= '{ODBC Driver 17 for SQL Server}'

conn = None
app = Flask(__name__)

app.logger.addHandler(logging.StreamHandler(sys.stdout))
app.logger.setLevel(logging.ERROR)
logger = logging.getLogger()

def database_connection():
   conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
   return conn

conn = database_connection()

@app.route('/api/v1/shop-based-recommend', methods = ['POST'])
def get_shop_based_fuzzy_filter():
    global conn
    if request.method == 'POST':
        product_in = request.json
        cate = product_in['Category']
        sellerID = product_in['Seller_ID']
        description = product_in['Mota']
        query = "select * from item where Seller_ID = " + str(sellerID)
        shop_products = pd.read_sql(query, conn)
        filtered_products = pd.DataFrame(process.extract(description, shop_products['Mota'], scorer = fuzz.token_sort_ratio, limit = 100), columns=("matchString", "matchScore", "index"))
        if(filtered_products.empty):
            return Response(filtered_products.to_json(orient= "records"), mimetype = 'application/json')
        else:
            sub_index = filtered_products['index'].values
            df_filtered = shop_products.iloc[sub_index]
            return Response(df_filtered.to_json(orient= "records"), mimetype = 'application/json')

@app.route('/api/v1/one-category-based-recommend', methods = ['POST'])
def get_category_basedfuzzy_filter():
    global conn
    if request.method == 'POST':
        product_in = request.json
        cate = product_in['Category']
        sellerID = product_in['Seller_ID']
        description = product_in['Mota']
        query = "select * from item where Category = '" + cate + "'"
        shop_products = pd.read_sql(query, conn)
        filtered_products = pd.DataFrame(process.extract(description, shop_products['Mota'], scorer = fuzz.token_sort_ratio, limit = 100), columns=("matchString", "matchScore", "index"))
        if(filtered_products.empty):
            return Response(filtered_products.to_json(orient= "records"), mimetype = 'application/json')
        else:
            sub_index = filtered_products['index'].values
            df_filtered = shop_products.iloc[sub_index]
            return Response(df_filtered.to_json(orient= "records"), mimetype = 'application/json')

if __name__ == '__main__':
    #model = joblib.load(open('results.pkl', 'rb'))
    conn = database_connection()
    app.run(debug=True)