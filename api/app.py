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


from flask import Flask, render_template, request, send_file, g
from io import BytesIO
import pandas as pd
from dotenv import load_dotenv
import jwt
from datetime import datetime, timedelta
import logging
from etl import FieldMapper
from pipelines import MongoDBConnector

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
