import openai
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
class EntityProcessor:
    def __init__(self):
        openai.api_key = "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"
        self.entity_types = ["Person Names", "Organization", "Hash Tags", "Location", "Brand", "Category", "URLs"]

    def extract_entities(self, message):
        try:
            # print(message)
            # Use the OpenAI API to perform zero-shot NER on the message
            prompt = f"Please extract the following entity types from the text: {', '.join(self.entity_types)}. \n\nText: {message}"
            response = openai.Completion.create(
                engine="gpt-3.5-turbo-instruct",
                prompt=prompt,
                max_tokens=1024,
                n=1,
                stop=None,
                temperature=0.5,
            )
            # Extract the entity labels and scores from the API response
            entities = response["choices"][0]["text"].split("\n")
            entity_dict = {et: None for et in self.entity_types}
            for entity in entities:
                for et in self.entity_types:
                    if et in entity:
                        entity_dict[et] = entity.split(":")[1].strip()
            # Return the extracted entities as a dictionary
            return entity_dict
        except Exception as e:
            print(f"Error during entity extraction: {e}")
            return None

    def process_entities(self, df, chunk_size=50):
        try:
            # Split the DataFrame into chunks for parallel processing
            chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

            # Create a ThreadPoolExecutor for parallel processing
            with ThreadPoolExecutor() as executor:
                # Process each chunk in parallel
                results = list(executor.map(self.apply_extraction, chunks))

            # Concatenate the results back into the original DataFrame
            df = pd.concat(results, ignore_index=True)

            # Drop rows where 'extracted_entities' is None
            df = df.dropna(subset=['extracted_entities'])

            # Convert the list of entity dictionaries into a DataFrame
            entity_df = pd.DataFrame(df['extracted_entities'].tolist(), columns=self.entity_types)

            # Concatenate the original DataFrame with the entity DataFrame
            df = pd.concat([df, entity_df], axis=1)

            print("Entity processing completed successfully.")
            return df

        except Exception as e:
            print(f"Error during entity processing: {e}")
            return df  # Return the original DataFrame if an error occurs

    def apply_extraction(self, chunk):
        # Apply the extract_entities function to each row in the chunk
        chunk['extracted_entities'] = chunk['Message'].apply(self.extract_entities)
        return chunk
        # except Exception as e:
        #     print(f"Error during entity processing: {e}")
        #     return df

# entity_processor = EntityProcessor()
# processed_df = entity_processor.process_entities(df)

# # Print the final DataFrame
# print(processed_df.head(30))
