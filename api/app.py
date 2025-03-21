from flask import Flask, render_template, request, send_file, g, jsonify
from io import BytesIO
import pandas as pd
from dotenv import load_dotenv
import jwt
from datetime import datetime, timedelta
import logging
from api.extract_transfer_load import FieldMapper
from api.pipelines import MongoDBConnector
from api.data_processor import DataProcessor
from api.text_classifier import TextClassifier
from api.entityprocessor import EntityProcessor
from api.main import run_data_processing_workflow
from api.settings import COLLECTION_POST

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
app.config.from_pyfile('config.py')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_authentication_token():
    """
    Generates a JWT token for user authentication.

    The token includes the current date and hour, an expiration time of 1 hour,
    and an audience claim for additional security.

    Returns:
        str: A JWT token encoded with the app's secret key.
    """
    current_datetime = datetime.utcnow()
    current_date_hour = current_datetime.strftime('%Y-%m-%d %H')

    payload = {
        'date_hour': current_date_hour,
        'exp': current_datetime + timedelta(hours=1),
        'aud': 'your_app_name'
    }

    return jwt.encode(payload, app.config['SECRET_KEY'], algorithm='HS256')


def authenticate_user(auth_token):
    """
    Authenticates a user by validating the provided JWT token.

    Args:
        auth_token (str): The JWT token to validate.

    Returns:
        bool: True if the token is valid, False otherwise.
    """
    try:
        jwt.decode(auth_token, app.config['SECRET_KEY'], algorithms=['HS256'], audience='your_app_name')
        return True
    except jwt.ExpiredSignatureError:
        return False
    except jwt.InvalidTokenError:
        return False


@app.before_request
def before_request():
    """
    Executes before each request to initialize the MongoDB client and store it in the Flask global object `g`.
    """
    g.mongo_client = MongoDBConnector()


@app.route('/trigger_daily_process', methods=['POST'])
def trigger_daily_process():
    """
    Triggers a daily process after validating the user's authentication token.

    Returns:
        str: A success message if the process is triggered, or an error message if authentication fails.
    """
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
    """
    Handles file uploads, processes the file, and uploads the data to MongoDB.

    Returns:
        str: Rendered HTML template with the result of the file upload process.
    """
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
    """
    Downloads data from MongoDB as an Excel file.

    Returns:
        File: An Excel file containing the data, or an error message if the download fails.
    """
    try:
        client = g.mongo_client
        collection = client.db[COLLECTION_POST]
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


@app.route('/run_process', methods=['POST'])
def run_process():
    """
    Triggers the data processing workflow and returns the status.

    Returns:
        JSON: A JSON response indicating the success or failure of the data processing workflow.
    """
    try:
        run_data_processing_workflow()
        return jsonify({"status": "success", "message": "Data processing completed successfully!"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=False)
