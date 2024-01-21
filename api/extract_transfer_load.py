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
from datetime import datetime
from dateutil import parser

from api.settings import *


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
    def format_timestamp_auto(self, input_timestamp_str):
        try:
            possible_formats = ["%Y-%m-%dT%H:%M:%S.%fZ", "%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S"]

            for timestamp_format in possible_formats:
                try:
                    dt_object = datetime.strptime(input_timestamp_str, timestamp_format)
                    formatted_timestamp = dt_object.strftime("%d-%m-%Y %H:%M:%S")
                    return formatted_timestamp

                except ValueError:
                    continue

            raise ValueError("Unable to determine timestamp format")

        except Exception as e:
            try:
                dt_object = parser.parse(str(input_timestamp_str))
                formatted_timestamp = dt_object.strftime("%d-%m-%Y %H:%M:%S")
                return formatted_timestamp

            except ValueError:
                raise ValueError("Unable to determine timestamp format with datetime and dateutil.parser")



    def detect_source(self, df):
        try:
            if all(field in df.columns for field in RIVAL_IQ):
                self.source = 'Rival IQ'
            elif all(field in df.columns for field in PHANTOM_BUSTER ):
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
                    
                    company_name = COMPANY_MAPPING.get(data['company'].lower(), data['company'].capitalize())

                    original_datetime = self.format_timestamp_auto(data['published_at'])
                    post_type = "Image" if data['post_type'].lower() == "photo" else "Video" if "video (linkedin source)" in data['post_type'].lower() else data['post_type'].capitalize()
                    Handle_Name=str(data['presence_handle']).capitalize() if data['presence_handle'] else ""
                        
                    
                    self.field_mapping = {
                        'Publish Date / Time':original_datetime,
                        'Company Name':company_name ,
                        'Social Media Channel': data['channel'],
                        'Handle Name': Handle_Name,
                        'Message': data['message'],
                        'Link': data['post_link'],
                        'Post Type': post_type,
                        'Like / applause': data['applause'],
                        'Comment / conversation': data['conversation'],
                        'Share / Repost / amplification': data['amplification'],
                        'Engagement': Engagement,
                        'Video Views': data['video_views'],
                        'Video Duration': '',
                        'Video Type': '',
                        'audience': data['audience'],
                    }
                    self.item.append(self.field_mapping)
            elif self.source == 'Phantom Buster':
                for data in data_dict:
                    company_Name=(data['profileUrl']
                                  .strip("/")
                                  .strip()
                                  .split("/")[-1]
                    )
                    company_name = COMPANY_MAPPING.get(company_Name.lower(), company_Name.capitalize())
                    post_type = "Image" if data['type'].lower() == "photo" else "Video" if "video (linkedin source)" in data['type'].lower() else data['type'].capitalize()

                    Engagement=(
                         data.get('likeCount',0)
                        +data.get('commentCount',0)
                        +data.get('repostCount',0)
                        )
                    original_datetime = self.format_timestamp_auto(data['postTimestamp'])
                    self.field_mapping = {
                        'Publish Date / Time': original_datetime,
                        'Company Name': company_name,
                        'Social Media Channel': 'LinkedIn',
                        'Handle Name': company_name,
                        'Message': data['postContent'],
                        'Link': data['postUrl'],
                        'Post Type': post_type,
                        'Like / applause': data['likeCount'],
                        'Comment / conversation': data['commentCount'],
                        'Share / Repost / amplification': data['repostCount'],
                        'Engagement': Engagement,
                        'Video Views': '',
                        'Video Duration': '',
                        'Video Type': '',
                        'audience': '',
                    }
                    self.item.append(self.field_mapping)
            return self.item
        except Exception as e:
            raise RuntimeError(f"Error mapping fields: {str(e)}")



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
