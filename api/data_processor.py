from pymongo import MongoClient
import pandas as pd
import pickle

class DataProcessor:
    def __init__(self):
        self.mongodb_uri = "mongodb+srv://lalsharath511:Sharathbhagavan15192142@legal.mosm3f4.mongodb.net/"
        self.database_name = "echo"
        self.client = MongoClient(self.mongodb_uri)
        self.db = self.client[self.database_name]
        self.keywords=self.fetch_data_from_mongo("keyword_data")
        

    def fetch_data_from_mongo(self,collection_name):
        try:
            collection = self.db[collection_name]
            cursor = collection.find()
            new_data = pd.DataFrame(list(cursor))
            return new_data
        except Exception as e:
            print(f"Error fetching data from MongoDB: {e}")
            return None
        
    def update_themes_subthemes(self,text, keyword_data):
        for keyword, theme, subtheme in zip(keyword_data['Keyword'], keyword_data['Theme'], keyword_data['Sub Theme']):
            keyword = keyword.lower().replace('#', '')  # Convert to lowercase and remove '#'
            if keyword in text.lower():  # Convert text to lowercase for case-insensitive matching
                return theme, subtheme, None  # Return None for subsubtheme
        return None, None, None
    
    def predict_labels(self, new_data):
        with open(r"api/model_q2.pkl", 'rb') as model_file:
            model_bytes = model_file.read()
        # Load the model from the bytes
        model = pickle.loads(model_bytes)
        tfidf_vectorizer = model['tfidf_vectorizer']
        gb_classifier = model['gb_classifier']
        new_data = new_data.apply(self.apply_keyword_matching, axis=1)
        new_data_tfidf = tfidf_vectorizer.transform(new_data['Message'])
        predicted_labels = gb_classifier.predict(new_data_tfidf)
        df = pd.DataFrame([x.strip().split('||') for x in predicted_labels], columns=['Themes', 'Subthemes', 'Subsubthemes'])

        # Merge the predictions with new_data
        new_data[['Themes', 'Subthemes', 'Subsubthemes']] = df[['Themes', 'Subthemes', 'Subsubthemes']]

        # # Reset the index of new_data before merging
        new_data.reset_index(drop=True, inplace=True)
        df = new_data
        df=self.categorize_duplicates(df)
        return df
        

    
    def apply_keyword_matching(self,row):
        data=self.keywords
        keyword_data=data.drop('_id', axis=1, errors='ignore')
        text = row['Message']
        theme, subtheme, subsubtheme = self.update_themes_subthemes(text, keyword_data)
        if theme is not None and subtheme is not None and subsubtheme is not None:
            row['Themes'] = theme
            row['Subthemes'] = subtheme
            row['Subsubthemes'] = subsubtheme
        return row
    def calculate_match_percentage(self,message1, message2):
        if pd.isna(message1) or pd.isna(message2):
            return 0.0

        words1 = set(message1.split())
        words2 = set(message2.split())

        if len(words1) == 0:
            return 0.0

        common_words = words1.intersection(words2)
        match_percentage = len(common_words) / len(words1)

        return match_percentage

    def categorize_duplicates(self,new_data, column_to_check="Message", match_threshold=0.8):
        # # Separate the predicted labels into different columns
        # df = pd.DataFrame([x.strip().split('||') for x in new_data['predicted_labels']],
        #                 columns=['Themes', 'Subthemes', 'Subsubthemes'])

        # # Merge the predictions with new_data
        # new_data[['Themes', 'Subthemes', 'Subsubthemes']] = df[['Themes', 'Subthemes', 'Subsubthemes']]

        # # Reset the index of new_data before merging
        # new_data.reset_index(drop=True, inplace=True)

        # Initialize the 'Tag' column with "Unique" for all records
        new_data['Tag'] = "Unique"

        # Initialize variables to keep track of categorized and uncategorized statements
        categorized_statements = set()
        uncategorized_statements = set(new_data.index)

        # Loop until all statements are categorized or moved to Unique and Duplicate tags
        while len(uncategorized_statements) > 0:
            statement1_index = uncategorized_statements.pop()
            statement1 = new_data.at[statement1_index, column_to_check]

            # Keep track of duplicate indices for statement1
            duplicate_indices = set()

            for statement2_index in list(uncategorized_statements):
                statement2 = new_data.at[statement2_index, column_to_check]

                match_percentage = self.calculate_match_percentage(statement1, statement2)

                if match_percentage >= match_threshold:
                    duplicate_indices.add(statement2_index)

            if len(duplicate_indices) > 0:
                # Mark statement1 as "Unique" if there are duplicates
                categorized_statements.add(statement1_index)
                for duplicate_index in duplicate_indices:
                    uncategorized_statements.remove(duplicate_index)
                    # Mark duplicates as "Duplicate"
                    new_data.at[duplicate_index, 'Tag'] = "Duplicate"
        self.client.close()
        return new_data