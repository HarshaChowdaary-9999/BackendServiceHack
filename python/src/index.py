from pymongo import MongoClient,errors
from flask import Flask,request,jsonify
from pymongo.collection import Collection
import requests
import pandas as pd
import io
import json
app = Flask(__name__)
import os

url=os.getenv('MONGO_URI', 'mongodb://localhost:27017')


try:
    databaseClient = MongoClient(url)
    db = databaseClient['sensorDataService']
    collection = db['record']
except errors.ConnectionError as e:
    raise Exception(f"Failed to connect to MongoDB: {str(e)}")

TotalRecordsInDatabase = 50000000
IngestFileSize = 5120
MaxRecords = 10000000
def TotalRowsInDatabase():
    if collection.count_documents({})  > TotalRecordsInDatabase:
        raise Exception("Service record limit exceeded")      

def getInputData(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.content

def insertData(csv_data):    
    df = pd.read_csv(io.StringIO(csv_data.decode('utf-8')))
    lenOfdf=len(df)
    if lenOfdf > MaxRecords:
        raise Exception("CSV record limit exceeded")
    convertedData = df.to_dict(orient='records')
    if lenOfdf +collection.count_documents({})  > TotalRecordsInDatabase:
        raise Exception("Service record limit exceeded")
    
    collection.insert_many(convertedData)

@app.route('/ingest', methods=['POST'])
def ingest():
    url = request.args.get('url')    
    if not url:
        return jsonify({'error': 'URL parameter is required'}), 400
    
    
    try:
        csv_data = getInputData(url)
        if len(csv_data) > IngestFileSize * 1024 * 1024:
            raise Exception("File size limit exceeded")

        TotalRowsInDatabase()

        insertData(csv_data)
        return jsonify({'message': 'Data ingested successfully'}), 200

    except requests.RequestException as e:
        return jsonify({'error': f'Failed to fetch CSV: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 400
def query(filter):
    
    queryf={}
    if filter.get('id'):
        queryf['id']=filter['id']
    if filter.get('type'):
       queryf['type']=filter['type']
    if filter.get('subtype'):
       queryf['subtype']=filter['subtype']
    if filter.get('location'):
        queryf['location']=filter['location']
    return queryf
def calMedian(data):
    readings=[record['reading'] for record in data]
    length=len(readings)
    mid=length//2
    if length%2 == 0:
        val=(readings[mid-1]+readings[mid])//2
    else:
        val=readings[mid]
    return {'count':length, 'median':val}
    

@app.route('/median', methods=['GET'])
def median():
    filter=json.loads(request.args.get('filter'))
    query_f=query(filter)
    
    try:
      try:
        data=collection.find(query_f)
      except :
        raise Exception('Could load the data from the database')
      
      if data:
        ans=calMedian(list(data))
      else:
        raise Exception('No data found in the database')
      return jsonify(ans),200
    except Exception as e:
      return jsonify({'error':str(e)}),400
    
    return jsonify({'message':'check'})
    

if __name__ == '__main__':
    app.run(debug=True)
