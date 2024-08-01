from .base_spider import BaseNewsSpider
from datetime import datetime
import pytz
from scrapy import Request
from ..firebase_manager import FirebaseManager

class LapsiSpider(BaseNewsSpider):
    name = 'lapsi'
    start_urls = [
        'https://lapsi.al/kategoria/te-fundit/'
    ]

    def __init__(self, *args, **kwargs):
        super(LapsiSpider, self).__init__(*args, **kwargs)
        self.article_count = {url: 0 for url in self.start_urls}
        self.firebase_manager = FirebaseManager()
        self.db = self.firebase_manager.client
        self.url_ledger = self.get_url_ledger()

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url, self.parse, dont_filter=True)

    def parse(self, response):
        articles = response.css('div#content article')
        category = response.url.split('/')[-2]

        for article in articles:
            if self.article_count[response.url] >= self.max_articles:
                continue

            link = article.css('div.post-content-wrapper a')
            article_url = link.css('::attr(href)').get()

            if article_url not in self.url_ledger.get(category, set()):
                thumbnail = article.css('img::attr(src)').get()
                
                yield response.follow(
                    article_url,
                    callback=self.parse_article,
                    meta={
                        'article_title': link.css('::text').get(),
                        'article_url': article_url,
                        'article_thumbnail': thumbnail,
                        'article_category': category
                    }
                )

                self.article_count[response.url] += 1
            else:
                self.logger.info(f"Skipping already scraped article: {article_url}")

    def parse_article(self, response):
        content = self.extract_content(response)
        return self.create_article_item(response, content)

    def get_content_elements(self, response):
        return response.css('div.entry-content > *')

    def parse_content_element(self, element):
        if element.root.tag == 'p':
            text = ''.join(element.css('*::text').getall())
            iframe = element.css('iframe::attr(src)').get()
            if iframe:
                return {'type': 'iframe', 'content': iframe}
            if text:
                return {'type': 'paragraph', 'content': text}
        elif element.root.tag == 'image':
            return {
                'type': 'image',
                'content': element.css('img::attr(src)').get(),
                'content_caption': element.css('figcaption::text').get()
            }
        elif element.root.tag == 'div' and 'wp-video' in element.css('div::attr(class)').get():
            return {'type': 'video', 'content': element.css('video source::attr(src)').get()}

    def get_article_image(self, response):
        return response.css('div.post-preview img::attr(src)').get()

    def get_published_date(self, response):
        date_str = response.css('div.entry-meta time.published::attr(datetime)').get()
        if date_str:
            dt = datetime.fromisoformat(date_str)
            dt_utc = dt.astimezone(pytz.UTC)
            return int(dt_utc.timestamp())
        return None