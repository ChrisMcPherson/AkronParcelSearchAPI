from flask import Flask, request
from flask_restful import Resource, Api
from flask_cors import CORS, cross_origin

from json import dumps
from json import loads
import os
import requests

import numpy as np
import pandas as pd

akron_open_data_url = 'http://192.168.4.110:3000/api'

app = Flask(__name__)
api = Api(app)
cors = CORS(app)
@app.route("/")


class Category(Resource):

  def get(self):
    try:
      response = requests.get(r"{0}/land_use_codes/classes.json".format(akron_open_data_url))
        # Consider any status other than 2xx an error
      if not response.status_code // 100 == 2:
          return "Error: Unexpected response {}".format(response)

      json_obj = response.json()
      return json_obj['data'][0]

    except requests.exceptions.RequestException as e:
        # A serious problem happened, like an SSLError or InvalidURL
        return "Error: {}".format(e)

class Lucs(Resource):

  def get(self, category):
    try:
      response = requests.get(r"{0}/land_use_codes.json?filters[use_class][]={1}".format(akron_open_data_url, category))
        # Consider any status other than 2xx an error
      if not response.status_code // 100 == 2:
          return "Error: Unexpected response {}".format(response)

      json_obj = response.json()
      # Remove first 4 characters to cleanup land use code name
      #lucs = pd.read_json(response.text)
      lucs = pd.read_json(dumps(json_obj['data']))
      lucs['label'] = lucs['label'].map(lambda x: str(x)[4:])
      return loads(lucs.to_json(orient='records'))

    except requests.exceptions.RequestException as e:
        # A serious problem happened, like an SSLError or InvalidURL
        return "Error: {}".format(e)


class ParcelData(Resource):

  def get(self, lucs):
    try:
      ###### Parse parameter string of land use codes into format for open data api
      if ',' in lucs:
        lucs = lucs.split(",")
        lucs_parater_string = "?"
        for i in lucs:
          parameter = "filters[use_code][]={0}&".format(i)
          lucs_parater_string = lucs_parater_string + parameter
      else:
        lucs_parater_string = "?filters[use_code][]={0}".format(lucs)

      ###### Retrieve all Parcel data for inputed land use codes
      parcel_url = "{0}/parcels.json?{1}".format(akron_open_data_url, lucs_parater_string)
      print(parcel_url)
      parcel_response = requests.get(parcel_url)
        # Consider any status other than 2xx an error
      if not parcel_response.status_code // 100 == 2:
          return "Error: Unexpected response {}".format(parcel_response)
      
      parcel_json = parcel_response.json()
      num_pages = parcel_json['meta']['pages']
      parcel_data = parcel_json['data']

      if num_pages > 1:
        #for page in range(2, num_pages + 1): # !! Artificially limiting responses
        for page in range(2,10):
            parcels = requests.get(parcel_url, params={'page': page})
            parcel_json = parcels.json()
            data = parcel_json['data']
            parcel_data.extend(data)

      
      ###### Retrieve all Parcel sales data for inputed land use codes
      sales_url = "{0}/sales.json?{1}".format(akron_open_data_url, lucs_parater_string)
      print(sales_url)
      sales_response = requests.get(sales_url)
        # Consider any status other than 2xx an error
      if not sales_response.status_code // 100 == 2:
          return "Error: Unexpected response {}".format(sales_response)

      sales_json = sales_response.json()
      num_pages = sales_json['meta']['pages']
      sales_data = sales_json['data']

      if num_pages > 1:
        #for page in range(2, num_pages + 1):
        for page in range(2, 10):
            sales = requests.get(sales_url, params={'page': page})
            sales_json = sales.json()
            data = sales_json['data']
            sales_data.extend(data)

      '''
      ##### Retrieve all Parcel appraisal data for inputed land use codes
      appraisal_response = requests.get("{0}/appraisals.json".format(akron_open_data_url))
        # Consider any status other than 2xx an error
      if not appraisal_response.status_code // 100 == 2:
          return "Error: Unexpected response {}".format(appraisal_response)

      appraisal_json = appraisal_response.json()

      '''

      # Return valuable data
      parcels = pd.read_json(dumps(parcel_data))
      parcels = parcels[['parcel_id','address','zip_code','acres','land_use_code_id']]

      sales = pd.read_json(dumps(sales_data))
      sales = sales[['parcel_id','price']]

      parcels = pd.merge(parcels, sales, on = ['parcel_id'], how = 'left')

      return loads(parcels.to_json(orient='records'))
      #return { "number":"1","address":"1234 st","postal":"44313","areaAcres":1,"areaSqft":4100,"lucId":1000,"appraisal":45,"lastSaleDate":"2017-04-12T00:00:00.000Z","lastSaleAmount":23,"pricePerSqft":35}
      
    except requests.exceptions.RequestException as e:
        # A serious problem happened, like an SSLError or InvalidURL
        return "Error: {}".format(e)


api.add_resource(Category, "/categories")
api.add_resource(Lucs, "/land_use_codes/<string:category>")
api.add_resource(ParcelData, "/parcels/<string:lucs>")

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=True)


    # curl http://localhost:8080/categories
    # curl http://localhost:8080/land_use_codes/C 
  # comma seperated string of land use codes
  # curl http://localhost:8080/parcels/209,299 