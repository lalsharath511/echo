from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import GradientBoostingClassifier
import re
import nltk
import pickle
import pandas as pd
from datetime import datetime

nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
class TextClassifier:
    def __init__(self, training_data=None):
        self.training_data = training_data
        self.tfidf_vectorizer = TfidfVectorizer()
        self.gb_classifier = GradientBoostingClassifier(n_estimators=100, random_state=42)


    def clean_text(self, text):
        if pd.notna(text):
            text = text.lower()
            text = re.sub(r'[^a-zA-Z\s]', '', text)
            tokens = word_tokenize(text)
            stop_words = set(stopwords.words('english'))
            tokens = [token for token in tokens if token not in stop_words]
            lemmatizer = WordNetLemmatizer()
            tokens = [lemmatizer.lemmatize(token) for token in tokens]
            cleaned_text = ' '.join(tokens)
            return cleaned_text
        else:
            return ''
    def load_model(file_path='model_q2.pkl'):
        # Load the model bytes from the file
        with open(file_path, 'rb') as model_file:
            model_bytes = model_file.read()
        # Load the model from the bytes
        loaded_model = pickle.loads(model_bytes)
        return loaded_model


    def update_themes_subthemes(self, text, keyword_data):
        for keyword, theme, subtheme in zip(keyword_data['Keyword'], keyword_data['Theme'], keyword_data['Sub Theme']):
            keyword = keyword.lower().replace('#', '')
            if keyword in text.lower():
                return theme, subtheme, None
        return None, None, None

    def train_classifier(self):
        if self.training_data is None:
            raise ValueError("Training data is not provided.")

        data = pd.DataFrame(self.training_data)

        required_columns = ['Vernon Sub Sub Theme', 'Vernon Sub Theme', 'Vernon Main Theme', 'Message']
        missing_columns = set(required_columns) - set(data.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

        data = data[pd.notnull(data['Vernon Sub Sub Theme'])]
        data = data[pd.notnull(data['Vernon Sub Theme'])]
        data = data[pd.notnull(data['Vernon Main Theme'])]
        data = data[pd.notnull(data['Message'])]
        data['Message'] = data['Message'].apply(self.clean_text)
        self.tfidf_vectorizer.fit(data['Message'])
        data_tfidf = self.tfidf_vectorizer.transform(data['Message'])
        data['Combined Themes'] = data['Vernon Main Theme'] + '||' + data['Vernon Sub Theme'] + '||' + data['Vernon Sub Sub Theme']
        self.gb_classifier.fit(data_tfidf, data['Combined Themes'])
        
    def auto_save_locally(self):
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path=f"model/model_{current_datetime}.pkl"
        self.train_classifier()  # Ensure the model is trained before saving
        model_data = {'tfidf_vectorizer': self.tfidf_vectorizer,
                      'gb_classifier': self.gb_classifier}

        with open(file_path, 'wb') as model_file:
            pickle.dump(model_data, model_file)

        print(f"Model saved locally at {file_path}")
        
# if __name__ == "__main__":
    
#     mongodb_uri = 'mongodb+srv://lalsharath511:Sharathbhagavan15192142@legal.mosm3f4.mongodb.net/'
#     database_name = 'echo'
#     collection_name = 'training_data'
#     client = MongoClient(mongodb_uri)
#     db = client[database_name]
#     collection = db[collection_name]

#     # Fetch data from MongoDB
#     cursor = collection.find({})
#     # main_data = pd.DataFrame(list(cursor))
#     training_data_list = list(cursor)  # Your list of dictionaries
#     classifier = TextClassifier(training_data=training_data_list)
#     classifier.auto_save_locally('model/model_q2.pkl')

