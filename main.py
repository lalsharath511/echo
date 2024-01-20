from flask import Flask, render_template, request, send_file
from io import BytesIO
import pandas as pd
from pymongo import MongoClient
from etl import FieldMapper

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    download_link = True

    if request.method == 'POST':
        # Check if the post request has the file part
        if 'file' not in request.files:
            return render_template('index.html', error='No file part', download_link=download_link)

        file = request.files['file']

        # If the user does not select a file, the browser also submits an empty file
        if file.filename == '':
            return render_template('index.html', error='No selected file', download_link=download_link)

        if file:
            # Process the file using FieldMapper without saving to disk
            try:
                file_contents = file.read()

                # Detect file type and read accordingly
                if file.filename.endswith('.xlsx'):
                    df = pd.read_excel(BytesIO(file_contents))
                elif file.filename.endswith('.csv'):
                    df = pd.read_csv(BytesIO(file_contents))
                else:
                    raise ValueError("Unsupported file format. Only .xlsx and .csv are supported.")

                mapper = FieldMapper(None)  # Pass None for file_path
                mapper.detect_source(df)
                mapper.map_fields(df)
                mapper.upload_to_mongodb('echo', 'transform_data')
                result = "Data uploaded to MongoDB successfully."
                download_link = True
            except RuntimeError as e:
                result = f"Error: {str(e)}"

            return render_template('index.html', result=result, download_link=download_link)

    return render_template('index.html', download_link=download_link)

@app.route('/download_data', methods=['GET'])
def download_data():
    try:
        # Retrieve all data from MongoDB collection
        client = MongoClient('mongodb+srv://lalsharath511:Sharathbhagavan15192142@legal.mosm3f4.mongodb.net/')
        db = client['echo']
        collection = db['transform_data']
        data_cursor = collection.find()
        data_list = list(data_cursor)

        # Convert data to DataFrame
        df = pd.DataFrame(data_list)

        # Save DataFrame to Excel file in memory
        excel_data = BytesIO()
        df.to_excel(excel_data, index=False)
        excel_data.seek(0)

        # Create a response with the Excel file
        return send_file(excel_data, as_attachment=True, download_name='exported_data.xlsx')

    except Exception as e:
        return f"Error: {str(e)}"


