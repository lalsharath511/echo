from pymongo import MongoClient,ASCENDING


def categorize_and_store_engagement_buckets():
    # Connect to MongoDB (adjust connection details as needed)
    client = MongoClient('mongodb+srv://lalsharath511:Sharathbhagavan15192142@legal.mosm3f4.mongodb.net/')
    db = client['echo']

    # Specify your input and output collections
    input_collection = db['transform_data']
    output_collection = db['engagement_buckets']

    # Create a unique index on 'document_id' if not already created
    output_collection.create_index([('document_id', ASCENDING)], unique=True)

    # Find all documents in the input collection
    all_documents = input_collection.find()

    # Categorize Engagement Buckets with Document IDs and store in output collection
    for document in all_documents:
        if 'engagement' in document:
            engagement_value = document['engagement']
            document_id = document['_id']
            
            bucket = None
            if 0 <= engagement_value <= 100:
                bucket = '0-100 Engagement'
            elif 101 <= engagement_value <= 500:
                bucket = '101-500 Engagement'
            elif 501 <= engagement_value <= 1000:
                bucket = '501-1000 Engagement'
            elif engagement_value > 1000:
                bucket = '1000+ Engagement'
            
            if bucket:
                # Insert a new entry, and MongoDB will enforce the unique index
                output_collection.insert_one({
                    'bucket': bucket,
                    'document_id': document_id,
                    'document':document
                })

    # Close the database connection
    client.close()

# Example usage:


# Call the function
categorize_and_store_engagement_buckets()
