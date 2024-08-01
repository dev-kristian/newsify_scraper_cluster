from openai import OpenAI
import tiktoken
from scrapy.exceptions import DropItem

from newsify import spiders
from firebase_admin import firestore
from .firebase_manager import FirebaseManager
import time
from google.cloud.firestore_v1.vector import Vector
    
class OpenAIProcessingPipeline:
    def __init__(self, api_key):
        self.api_key = api_key
        self.openai_client = OpenAI(api_key=self.api_key)
        self.encoding = tiktoken.get_encoding("cl100k_base")

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            api_key=crawler.settings.get('OPENAI_API_KEY')
        )

    def process_item(self, item, spider):
        # Generate embeddings
        embeddings = self.get_embeddings(item)
        item['article_embeddings'] = embeddings

        # Generate summary if the content is long enough
        summary = self.get_summary(item)
        if summary:
            item['article_summary'] = summary

        return item

    def get_embeddings(self, item):
        # Concatenate title and paragraphs
        text_to_embed = item['article_title'] + " " + " ".join(
            [content['content'] for content in item['article_content'] if content['type'] == 'paragraph']
        )
        
        # Encode the text
        encoded_text = self.encoding.encode(text_to_embed)
        
        # Truncate to 8000 tokens if necessary
        if len(encoded_text) > 8000:
            encoded_text = encoded_text[:8000]
            text_to_embed = self.encoding.decode(encoded_text)
        
        # Get embeddings from OpenAI
        response = self.openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=text_to_embed,
            encoding_format="float",
            dimensions=512  
        )
        
        return response.data[0].embedding

    def get_summary(self, item):
        # Concatenate only paragraphs
        text_to_summarize = " ".join(
            [content['content'] for content in item['article_content'] if content['type'] == 'paragraph']
        )
        
        # Encode the text
        encoded_text = self.encoding.encode(text_to_summarize)
        
        # Check if the text is more than 300 tokens
        if len(encoded_text) <= 300:
            return None
        
        # Generate summary using OpenAI API
        try:
            completion = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Ju jeni një asistent i dobishëm që përmbledh artikujt e lajmeve."},
                    {"role": "user", "content": f"Ju lutemi përmblidheni artikullin e mëposhtëm të lajmit në një paragraf të përmbledhur:\n\n{text_to_summarize}"}
                ]
            )
            return completion.choices[0].message.content
        except Exception as e:
            spiders.logger.error(f"Error generating summary: {str(e)}")
            return None

class ArticleValidationPipeline:
    def process_item(self, item, spider):
        if not item['article_content']:
            raise DropItem("Article content is empty")
        return item
    
class FirestorePipeline:
    def __init__(self):
        self.firebase_manager = FirebaseManager()
        self.db = self.firebase_manager.client

    def process_item(self, item, spider):
        source_name = spider.name.split('_')[0].lower()

        # Get a reference to the source document
        source_doc_ref = self.db.collection('news_sources').document(source_name)

        # Create a reference to the article document in the subcollection
        doc_ref = source_doc_ref.collection('articles').document()

        # Prepare the article data
        article_data = {
            'article_title': item['article_title'],
            'article_url': item['article_url'],
            'article_thumbnail': item['article_thumbnail'],
            'article_image': item['article_image'],
            'article_content': item['article_content'],
            'article_published_date': item['article_published_date'],
            'article_category': item['article_category'],
            'article_embeddings': Vector(item['article_embeddings']),
            'cluster_id':-1
        }

        if 'article_summary' in item:
            article_data['article_summary'] = item['article_summary']

        # Save the article
        doc_ref.set(article_data)

        # Update the URL ledger
        self.update_url_ledger(source_doc_ref, item['article_url'], item['article_category'])

        # Update the article stats within the source document
        self.update_stats(source_doc_ref, item['article_category'])

        return item

    def update_url_ledger(self, source_doc_ref, url, category):
        current_date = int(time.time())
        current_day = current_date - (current_date % 86400)  # Round down to the start of the day

        ledger_ref = source_doc_ref.collection('url_ledger').document(str(current_day))
        ledger_ref.set({
            category: firestore.ArrayUnion([url])
        }, merge=True)

    def update_stats(self, source_doc_ref, category):
        # Update the article stats within the source document
        source_doc_ref.set({
            'article_stats': {
                'total_articles': firestore.Increment(1),
                f'category_{category}': firestore.Increment(1)
            }
        }, merge=True)