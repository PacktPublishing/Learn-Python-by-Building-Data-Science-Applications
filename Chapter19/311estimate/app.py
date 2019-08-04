from chalice import Chalice, Response
import json, urllib.request
url = 'https://raw.githubusercontent.com/PacktPublishing/Python-Programming-Projects-Learn-Python-3.7-by-building-applications/master/Chapter18/data/model.json'

obj = urllib.request.urlopen(url).read()
model = json.loads(obj)


app = Chalice(app_name='311prediction')



@app.route('/predict/{complaint_type}', methods=['GET'])
def predict(complaint_type:str) -> Response:
    
    if complaint_type in model:
        return Response(status_code=200,
                        headers={'Content-Type': 'application/json'},
                        body={'status': 'success',
                              'complaint_type': complaint_type,
                              'estimated_time': model[complaint_type]})
    else:
        return Response(status_code=400,
                        headers={'Content-Type': 'application/json'},
                        body={'status': 'failure',
                              'problem': 'Complaint type is not in database',
                              'complaint_type': complaint_type})


@app.schedule('rate(1 hour)')
def collect_311(event):
    print(event.to_dict())