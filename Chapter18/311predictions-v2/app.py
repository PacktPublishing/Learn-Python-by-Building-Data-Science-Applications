import pickle

import boto3
import pandas as pd
from chalice import Chalice, Response
from sklearn.base import BaseEstimator
from ml import TimeTransformer

S3 = boto3.client('s3', region_name='us-east-1')
BUCKET, KEY = 'philipp-packt', 'model.pkl'

response = S3.get_object(Bucket=BUCKET, Key=KEY)
body = response['Body'].read()
model = pickle.loads(body)

app = Chalice(app_name='311predictions-v2')
singleton = pd.DataFrame([{'complaint_type':'dummy', 
                           'latitude':1.1111, 
                           'longitude':1.1111,
                           'created_date':pd.to_datetime('2019-01-01')}])

mapping = {
    'lon': 'longitude',
    'lat': 'latitude',
    'date': 'created_date'
}

dtypes = {
    'lon': float,
    'lat': float,
    'date': pd.to_datetime
}

@app.route('/predict/{complaint_type}', methods=['GET'])
def index(complaint_type:str):
    singleton.loc[0, 'complaint_type'] = complaint_type

    for k, col in mapping.items():
        singleton.loc[0, col] = dtypes[k](app.current_request.query_params.get(k, pd.np.nan))

    try:
        prediction = model.predict(singleton[['complaint_type', 'latitude', 'longitude','created_date']])[0]

        return Response(status_code=200,
                        headers={'Content-Type': 'application/json'},
                        body={'status': 'success',
                              'estimated_time': prediction})
    except Exception as e:
        return Response(status_code=400,
                        headers={'Content-Type': 'application/json'},
                        body={'status': 'failure',
                              'features': singleton.astype(str).to_dict(orient='records')[0],
                              'inbound': app.current_request.query_params,
                              'error message': str(e)})