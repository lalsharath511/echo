# from flask import Flask, render_template, request, send_file
# from io import BytesIO
# import pandas as pd
# from pymongo import MongoClient
# from dotenv import load_dotenv
# import jwt
# from datetime import datetime, timedelta
# import os
# # from etl import FieldMapper
# from pipelines import MongoDBConnector

# load_dotenv()

# app = Flask(__name__)
# SECRET_KEY = os.getenv('SECRET_KEY')


# def generate_authentication_token():
#     current_datetime = datetime.utcnow()
#     current_date_hour = current_datetime.strftime('%Y-%m-%d %H')

#     payload = {
#         'date_hour': current_date_hour,
#         'exp': current_datetime + timedelta(hours=1)
#     }

#     token = jwt.encode(payload, SECRET_KEY, algorithm='HS256')

#     return token

# def authenticate_user(auth_token):
#     try:
#         payload = jwt.decode(auth_token, SECRET_KEY, algorithms=['HS256'])
#         return True
#     except jwt.ExpiredSignatureError:
#         return False
#     except jwt.InvalidTokenError:
#         return False

# @app.route('/trigger_daily_process', methods=['POST'])
# def trigger_daily_process():
#     try:
#         # Get the authentication token from the request header
#         auth_token = request.headers.get('Authorization')

#         # Authenticate the user
#         if not authenticate_user(auth_token):
#             return "Invalid or expired authentication token.", 401

#         # Get the payload (today's date) from the request
#         payload = request.get_json()
#         today_date = payload.get('today_date')

#         # Your logic here for the daily process
#         # For example, use today_date in your processing logic

#         result = f"Daily process triggered successfully for date: {today_date}"

#         return result

#     except Exception as e:
#         return f"Error: {str(e)}"

# @app.route('/', methods=['GET', 'POST'])
# def upload_file():
#     download_link = True
#     mapper = FieldMapper(None)
#     client=MongoDBConnector() 


#     if request.method == 'POST':
#         # Check if the post request has the file part
#         if 'file' not in request.files:
#             return render_template('index.html', error='No file part', download_link=download_link)

#         file = request.files['file']

#         # If the user does not select a file, the browser also submits an empty file
#         if file.filename == '':
#             return render_template('index.html', error='No selected file', download_link=download_link)

#         if file:
#             # Process the file using FieldMapper without saving to disk
#             try:
#                 file_contents = file.read()
                

#                 # Detect file type and read accordingly
#                 if file.filename.endswith('.xlsx'):
#                     df = pd.read_excel(BytesIO(file_contents))
#                 elif file.filename.endswith('.csv'):
#                     df = pd.read_csv(BytesIO(file_contents))
#                 else:
#                     raise ValueError("Unsupported file format. Only .xlsx and .csv are supported.")
#                 mapper.detect_source(df)
#                 data=mapper.map_fields(df)
#                 filename=file.filename
#                 client.upload_elt_to_mongo(data,filename)
#                 result = "Data uploaded to MongoDB successfully."
#                 download_link = True
#             except RuntimeError as e:
#                 result = f"Error: {str(e)}"

#             return render_template('index.html', result=result, download_link=download_link)

#     return render_template('index.html', download_link=download_link)

# @app.route('/download_data', methods=['GET'])
# def download_data():
#     try:
#         # Retrieve all data from MongoDB collection
#         client=MongoDBConnector() 
#         collection = client.db['transform_data']
#         data_cursor = collection.find()
#         data_list = list(data_cursor)
#         # Convert data to DataFrame
#         df = pd.DataFrame(data_list)
#         if "metadata_id" in df.columns:
#             df = df.drop(columns=["metadata_id"])

#         excel_data = BytesIO()
#         df.to_excel(excel_data, index=False)
#         excel_data.seek(0)

#         # Create a response with the Excel file
#         return send_file(excel_data, as_attachment=True, download_name='exported_data.xlsx')

#     except Exception as e:
#         return f"Error: {str(e)}"


# if __name__ == '__main__':
#     app.run(debug=False)
# # if __name__ == '__main__':
# #     app.run(host='0.0.0.0', port=5000)


from flask import Flask, render_template, request, send_file, g,jsonify
from io import BytesIO
import pandas as pd
from dotenv import load_dotenv
import jwt
from datetime import datetime, timedelta
import logging
# import sys
# sys.path.append(".")
import pandas as pd
from api.extract_transfer_load import FieldMapper
from api.pipelines import MongoDBConnector
from api.data_processor import *
from api.text_classifier import *
from api.entityprocessor import *


load_dotenv()

app = Flask(__name__)
app.config.from_pyfile('config.py')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_authentication_token():
    current_datetime = datetime.utcnow()
    current_date_hour = current_datetime.strftime('%Y-%m-%d %H')

    payload = {
        'date_hour': current_date_hour,
        'exp': current_datetime + timedelta(hours=1),
        'aud': 'your_app_name'
    }

    token = jwt.encode(payload, app.config['SECRET_KEY'], algorithm='HS256')

    return token

def authenticate_user(auth_token):
    try:
        payload = jwt.decode(auth_token, app.config['SECRET_KEY'], algorithms=['HS256'], audience='your_app_name')
        return True
    except jwt.ExpiredSignatureError:
        return False
    except jwt.InvalidTokenError:
        return False

@app.before_request
def before_request():
    g.mongo_client = MongoDBConnector()

@app.route('/trigger_daily_process', methods=['POST'])
def trigger_daily_process():
    try:
        auth_token = request.headers.get('Authorization')

        if not authenticate_user(auth_token):
            return "Invalid or expired authentication token.", 401

        payload = request.get_json()
        today_date = payload.get('today_date')

        result = f"Daily process triggered successfully for date: {today_date}"
        logger.info(result)
        return result

    except Exception as e:
        error_message = f"Error: {str(e)}"
        logger.error(error_message)
        return error_message, 500

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    download_link = True
    mapper = FieldMapper(None)
    client = g.mongo_client

    if request.method == 'POST':
        if 'file' not in request.files:
            return render_template('index.html', error='No file part', download_link=download_link)

        file = request.files['file']

        if file.filename == '':
            return render_template('index.html', error='No selected file', download_link=download_link)

        if file:
            try:
                file_contents = file.read()

                if file.filename.endswith('.xlsx'):
                    df = pd.read_excel(BytesIO(file_contents))
                elif file.filename.endswith('.csv'):
                    df = pd.read_csv(BytesIO(file_contents))
                else:
                    raise ValueError("Unsupported file format. Only .xlsx and .csv are supported.")
                
                mapper.detect_source(df)
                data = mapper.map_fields(df)
                filename = file.filename
                client.upload_elt_to_mongo(data, filename)
                result = "Data uploaded to MongoDB successfully."
                download_link = True
                logger.info(result)
            except RuntimeError as e:
                result = f"Error: {str(e)}"
                logger.error(result)

            return render_template('index.html', result=result, download_link=download_link)

    return render_template('index.html', download_link=download_link)

@app.route('/handle_post_request', methods=['POST'])
def handle_post_request():
    try:
        # data = request.get_json()
        data_processor = DataProcessor()
        text_classifier = TextClassifier()
        entity_processor = EntityProcessor()
        new_data = data_processor.fetch_data_from_mongo(collection_name='transform_data')
        new_data['transform_data_id'] = new_data['_id']
        new_data = new_data.drop('_id', axis=1, errors='ignore')
        new_data['Message'] = new_data['Message'].apply(text_classifier.clean_text)
        # new_data = new_data.apply(data_processor.apply_keyword_matching, axis=1)
        # model=text_classifier.load_model(file_path="model/model_q2.pkl")
        df1=data_processor.predict_labels(new_data)
        processed_df = entity_processor.process_entities(df=df1)
        # Do something with the incoming JSON data
        json_response = processed_df.to_json(orient='records')
        response_data = {'data': json_response}
            # Respond with the JSON representation of the DataFrame
        return jsonify(json_response)

    except Exception as e:
        error_message = f'Error: {str(e)}'
        return jsonify({'error': error_message}), 500

@app.route('/download_data', methods=['GET'])
def download_data():
    try:
        client = g.mongo_client
        collection = client.db['transform_data']
        data_cursor = collection.find()
        data_list = list(data_cursor)

        df = pd.DataFrame(data_list)
        if "metadata_id" in df.columns:
            df = df.drop(columns=["metadata_id"])

        excel_data = BytesIO()
        df.to_excel(excel_data, index=False)
        excel_data.seek(0)

        return send_file(excel_data, as_attachment=True, download_name='exported_data.xlsx')

    except Exception as e:
        error_message = f"Error: {str(e)}"
        logger.error(error_message)
        return error_message, 500

if __name__ == '__main__':
    app.run(debug=False)
