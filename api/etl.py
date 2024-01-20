# import pandas as pd
# import json
# from pymongo import MongoClient

# class FieldMapper:
#     def __init__(self, file_path):
#         self.file_path = file_path
#         self.field_mapping = {}
#         self.source = None
#         self.item=[]

#     def read_file(self):
#         try:
#             if self.file_path.endswith('.xlsx'):
#                 df = pd.read_excel(self.file_path)
#             elif self.file_path.endswith('.csv'):
#                 df = pd.read_csv(self.file_path)
#             else:
#                 raise ValueError("Unsupported file format. Only .xlsx and .csv are supported.")
#         except Exception as e:
#             raise RuntimeError(f"Error reading file: {str(e)}")

#         return df

#     def detect_source(self, df):
#         try:
#             rival_iq_fields = [
#                 "published_at", "report_generated_at", "captured_at", "company", "channel",
#                 "presence_handle", "message", "post_link", "link", "link_title", 
#                 "link_description", "image", "post_type", "posted_domain", "posted_url", 
#                 "engagement_total", "applause", "conversation", "amplification", "audience", 
#                 "engagement_rate_by_follower", "engagement_rate_lift", "post_tag_ugc", 
#                 "post_tag_contests"
#             ]

#             phantom_buster_fields = [
#                 "postUrl", "imgUrl", "type", "postContent", "likeCount",
#                 "commentCount", "repostCount", "postDate", "action",
#                 "profileUrl", "timestamp", "postTimestamp", "videoUrl",
#                 "sharedPostUrl"
#             ]

#             if all(field in df.columns for field in rival_iq_fields):
#                 self.source = 'Rival IQ'
#             elif all(field in df.columns for field in phantom_buster_fields):
#                 self.source = 'Phantom Buster'
#             else:
#                 raise ValueError("Unable to determine the source file.")

#         except Exception as e:
#             raise RuntimeError(f"Error detecting source: {str(e)}")

#     def map_fields(self, df):
#         try:
#             data_dict = df.to_dict(orient='records')
#             if self.source == 'Rival IQ':
#                 for data in data_dict:
#                 # Map to JSON fields for Rival IQ
#                     self.field_mapping = {
#                         'Publish Date / Time': data['published_at'],
#                         'Company Name': data['company'],
#                         'Social Media Channel': data['channel'],
#                         'Handle Name': data['presence_handle'],
#                         'Message': data['message'],
#                         'Link': data['post_link'],
#                         'Post Type': data['post_type'],
#                         'Like / applause': data['applause'],
#                         'Comment / conversation': data['conversation'],
#                         'Share / Repost / amplification': data['amplification'],
#                         'Engagement': '',
#                         'Video Views': '',
#                         'Video Duration': '',
#                         'Video Type': '',
#                         'audience': data['audience']
#                     }
#                     self.item.append(self.field_mapping)
#             elif self.source == 'Phantom Buster':
#                 # Map to JSON fields for Phantom Buster
#                 for data in data_dict:
#                     self.field_mapping = {
#                         'Publish Date / Time': data['postTimestamp'],
#                         'Company Name': data['profileUrl'],
#                         'Social Media Channel': 'LinkedIn',
#                         'Handle Name': data['profileUrl'],
#                         'Message': data['postContent'],
#                         'Link': data['postUrl'],
#                         'Post Type': data['type'],
#                         'Like / applause': data['likeCount'],
#                         'Comment / conversation': data['commentCount'],
#                         'Share / Repost / amplification': data['repostCount'],
#                         'Engagement': '',
#                         'Video Views': '',
#                         'Video Duration': '',
#                         'Video Type': '',
#                         'audience': ''
#                     }
#                     self.item.append(self.field_mapping)

                
#         except Exception as e:
#             raise RuntimeError(f"Error mapping fields: {str(e)}")

#     def upload_to_mongodb(self, df, database_name, collection_name):
#         try:
#             client = MongoClient('mongodb://username:password@localhost:27017/')  # Replace with your MongoDB connection string
#             db = client[database_name]
#             collection = db[collection_name]

#             # Set 'Primary Key' as index
#             # df.set_index('Primary Key', inplace=True)

#             # Convert DataFrame to dictionary for easy insertion
           
#             # for i in self.item:
#             #     print(i)
                
#             # print(data_dict)

#             # Insert data into MongoDB collection
#             collection.insert_many(self.item)

#         except Exception as e:
#             raise RuntimeError(f"Error uploading to MongoDB: {str(e)}")

# def main():
#     try:
#         file_path1 = 'Untitled.xlsx'  # Replace with the actual file path

#         mapper1 = FieldMapper(file_path1)
#         df1 = mapper1.read_file()
#         mapper1.detect_source(df1)
#         mapper1.map_fields(df1)
#         mapper1.upload_to_mongodb(df1, 'your_database_name', 'your_collection_name')

#         print("Data uploaded to MongoDB successfully.")

#     except RuntimeError as e:
#         print(f"Error: {str(e)}")

# if __name__ == "__main__":
#     main()






import pandas as pd
from pymongo import MongoClient

class FieldMapper:
    def __init__(self, file_path):
        self.file_path = file_path
        self.field_mapping = {}
        self.source = None
        self.item = []

    def read_file(self):
        try:
            if self.file_path.endswith('.xlsx'):
                return pd.read_excel(self.file_path)
            elif self.file_path.endswith('.csv'):
                return pd.read_csv(self.file_path)
            else:
                raise ValueError("Unsupported file format. Only .xlsx and .csv are supported.")
        except Exception as e:
            raise RuntimeError(f"Error reading file: {str(e)}")

    def detect_source(self, df):
        rival_iq_fields = [
                "published_at", "report_generated_at", "captured_at", "company", "channel",
                "presence_handle", "message", "post_link", "link", "link_title", 
                "link_description", "image", "post_type", "posted_domain", "posted_url", 
                "engagement_total", "applause", "conversation", "amplification", "audience", 
                "engagement_rate_by_follower", "engagement_rate_lift", "post_tag_ugc", 
                "post_tag_contests"
            ]

        phantom_buster_fields = [
                "postUrl", "imgUrl", "type", "postContent", "likeCount",
                "commentCount", "repostCount", "postDate", "action",
                "profileUrl", "timestamp", "postTimestamp", "videoUrl",
                "sharedPostUrl"
            ]
        
        try:
            if all(field in df.columns for field in rival_iq_fields):
                self.source = 'Rival IQ'
            elif all(field in df.columns for field in phantom_buster_fields):
                self.source = 'Phantom Buster'
            else:
                raise ValueError("Unable to determine the source file.")
        except Exception as e:
            raise RuntimeError(f"Error detecting source: {str(e)}")

    def map_fields(self, df):
        try:
            data_dict = df.to_dict(orient='records')
            if self.source == 'Rival IQ':
                for data in data_dict:
                    Engagement=(
                         data.get('applause',0)
                        +data.get('conversation',0)
                        +data.get('amplification',0)
                        )
                    self.field_mapping = {
                        'Publish Date / Time': data['published_at'],
                        'Company Name': data['company'],
                        'Social Media Channel': data['channel'],
                        'Handle Name': data['presence_handle'],
                        'Message': data['message'],
                        'Link': data['post_link'],
                        'Post Type': data['post_type'],
                        'Like / applause': data['applause'],
                        'Comment / conversation': data['conversation'],
                        'Share / Repost / amplification': data['amplification'],
                        'Engagement': Engagement,
                        'Video Views': '',
                        'Video Duration': '',
                        'Video Type': '',
                        'audience': data['audience']
                    }
                    self.item.append(self.field_mapping)
            elif self.source == 'Phantom Buster':
                for data in data_dict:
                    Company_Name=(data['profileUrl']
                                  .strip("/")
                                  .strip()
                                  .split("/")[-1]
                    )
                    Engagement=(
                         data.get('likeCount',0)
                        +data.get('commentCount',0)
                        +data.get('repostCount',0)
                        )
                    self.field_mapping = {
                        'Publish Date / Time': data['postTimestamp'],
                        'Company Name': Company_Name,
                        'Social Media Channel': 'LinkedIn',
                        'Handle Name': Company_Name,
                        'Message': data['postContent'],
                        'Link': data['postUrl'],
                        'Post Type': data['type'],
                        'Like / applause': data['likeCount'],
                        'Comment / conversation': data['commentCount'],
                        'Share / Repost / amplification': data['repostCount'],
                        'Engagement': Engagement,
                        'Video Views': '',
                        'Video Duration': '',
                        'Video Type': '',
                        'audience': ''
                    }
                    self.item.append(self.field_mapping)
        except Exception as e:
            raise RuntimeError(f"Error mapping fields: {str(e)}")

    def upload_to_mongodb(self, database_name, collection_name):
        try:
            client = MongoClient('mongodb+srv://lalsharath511:Sharathbhagavan15192142@legal.mosm3f4.mongodb.net/')  # Replace with your MongoDB connection string
            db = client[database_name]
            collection = db[collection_name]
            collection.insert_many(self.item)
            # print(self.item[1])
        except Exception as e:
            raise RuntimeError(f"Error uploading to MongoDB: {str(e)}")

# def main():
#     try:
#         file_path = 'Untitled spreadsheet.xlsx'  # Replace with the actual file path
#         mapper = FieldMapper(file_path)
#         df = mapper.read_file()
#         mapper.detect_source(df)
#         mapper.map_fields(df)
        

#         mapper.upload_to_mongodb('newclient', 'test')
#         print("Data uploaded to MongoDB successfully.")
#     except RuntimeError as e:
#         print(f"Error: {str(e)}")

# if __name__ == "__main__":
#     main()
