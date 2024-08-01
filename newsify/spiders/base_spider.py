import scrapy
from abc import ABC, abstractmethod
import time

class BaseNewsSpider(scrapy.Spider, ABC):
    name = 'base_news'
    article_count = 0
    max_articles = 5
    
    def __init__(self, *args, **kwargs):
        super(BaseNewsSpider, self).__init__(*args, **kwargs)

    @abstractmethod
    def parse(self, response):
        pass

    @abstractmethod
    def parse_article(self, response):
        pass

    def extract_content(self, response):
        content = []
        for element in self.get_content_elements(response):
            item = self.parse_content_element(element)
            if item:
                content.append(item)
        return content

    @abstractmethod
    def get_content_elements(self, response):
        pass

    @abstractmethod
    def parse_content_element(self, element):
        pass

    def create_article_item(self, response, content):
        return {
            'article_title': response.meta.get('article_title'),
            'article_url': response.meta.get('article_url'),
            'article_thumbnail': response.meta.get('article_thumbnail'),
            'article_image': self.get_article_image(response),
            'article_content': content,
            'article_published_date': self.get_published_date(response),
            'article_category': response.meta.get('article_category'),
            'cluster_id':-1
        }
        
    @abstractmethod
    def get_article_image(self, response):
        pass

    @abstractmethod
    def get_published_date(self, response):
        pass

    def get_url_ledger(self):
        source_doc_ref = self.db.collection('news_sources').document(self.name)
        ledger = {}
        
        # Get the current day in Unix timestamp
        current_date = int(time.time())
        current_day = current_date - (current_date % 86400)  # Round down to the start of the day
        
        # Get the URL ledger document for the current day
        ledger_doc = source_doc_ref.collection('url_ledger').document(str(current_day)).get()
        
        if ledger_doc.exists:
            ledger = ledger_doc.to_dict()
        
        return ledger