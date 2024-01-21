from pymongo import MongoClient
from datetime import datetime

class MongoDBConnector:
    def __init__(self):
        self.connection_string = "mongodb+srv://lalsharath511:Sharathbhagavan15192142@legal.mosm3f4.mongodb.net/"
        self.database_name = "echo"
        self.client = MongoClient(self.connection_string)
        self.db = self.client[self.database_name]
        
    def upload_metadata(self, file_name, len_df):
        # Generate metadata for the uploaded file
        metadata = {
            'file_name': file_name,
            'upload_date': datetime.utcnow().strftime('%d-%m-%Y'),
            'upload_time': datetime.utcnow().strftime('%H:%M:%S'),
            'total_data_count': len_df,
        }

        # Upload metadata to MongoDB
        metadata_collection = self.db['metadata']  # Assuming 'metadata' is the collection name
        metadata_id = metadata_collection.insert_one(metadata).inserted_id
        return metadata_id
    def upload_elt_to_mongo(self,data,filename):
        try:
            metadata_id=self.upload_metadata(filename,len(data))
            updated_data = [{**item, "metadata_id": metadata_id} for item in data]
            collection = self.db["transform_data"]
            collection.insert_many(updated_data)
            
            # print(self.item[1])
        except Exception as e:
            raise RuntimeError(f"Error uploading to MongoDB: {str(e)}")


    def close_connection(self):
        try:
            if self.client:
                self.client.close()
                print("Connection to MongoDB closed.")
        except Exception as e:
            raise RuntimeError(f"Error closing MongoDB connection: {str(e)}")

# Example usage:
# if __name__ == '__main__':
#     # try:
#         # Replace with your MongoDB connection details
#         connection_string = "your_mongodb_connection_string"
#         database_name = "your_database_name"

#         # Create an instance of MongoDBConnector
#         mongo_connector = MongoDBConnector()
        

    #     # Your MongoDB operations here...

    # except RuntimeError as e:
    #     print(f"Error: {str(e)}")

    # finally:
    #     # Close the MongoDB connection in the finally block to ensure it gets closed even if an exception occurs
    #     if 'mongo_connector' in locals():
    #         mongo_connector.close_connection()
