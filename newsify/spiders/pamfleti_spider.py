from .base_spider import BaseNewsSpider
from datetime import datetime
import pytz
from ..firebase_manager import FirebaseManager

class PamfletiSpider(BaseNewsSpider):
    name = 'pamfleti'
    start_urls = [
        'https://pamfleti.net/category/aktualitet/'
    ]

    def __init__(self, *args, **kwargs):
        super(PamfletiSpider, self).__init__(*args, **kwargs)
        self.article_count = {url: 0 for url in self.start_urls}
        self.firebase_manager = FirebaseManager()
        self.db = self.firebase_manager.client
        self.url_ledger = self.get_url_ledger()

    def parse(self, response):
        articles = response.css('div.c-flexy.shtoketu article.a-card')
        category = response.url.split('/')[-2]

        for article in articles:
            if self.article_count[response.url] >= self.max_articles:
                continue

            title = article.css('h3.a-head::text').get()
            href = article.css('a::attr(href)').get()
            article_url = f"https://pamfleti.net" + href

            if article_url not in self.url_ledger.get(category, set()):
                thumbnail = article.css('img.a-media_img::attr(data-src)').get()

                yield response.follow(
                    article_url,
                    callback=self.parse_article,
                    meta={
                        'article_title': title,
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
        return response.css('div.all-content p, div.all-content div.kodiim')

    def parse_content_element(self, element):
        if element.root.tag == 'p':
            text = element.css('::text').get()
            iframe = element.css('iframe::attr(src)').get()
            image = element.css('img::attr(src)').get()
            
            if text:
                return {'type': 'paragraph', 'content': text}
            if iframe:
                return {'type': 'iframe', 'content': iframe}
            if image:
                return {'type': 'image', 'content': image}
        elif element.root.tag == 'div' and 'kodiim' in element.attrib.get('class', ''):
            iframe_src = element.css('iframe::attr(src)').get()
            if iframe_src:
                return {'type': 'iframe', 'src': iframe_src}

    def get_article_image(self, response):
        article_image = response.css('div.all-content div.horizontal.imazhiim img::attr(src)').get()
        if not article_image:
            p_tags = response.css('div.all-content p')
            for p_tag in p_tags:
                img_tag = p_tag.css('img')
                if img_tag:
                    article_image = img_tag.css('::attr(src)').get()
                    break
        return article_image

    def get_published_date(self, response):
        date_str = response.css('span.a-date::text').get()
        if date_str:
            # Dictionary to map Albanian month names to numbers
            albanian_months = {
                'Janar': 1, 'Shkurt': 2, 'Mars': 3, 'Prill': 4, 'Maj': 5, 'Qershor': 6,
                'Korrik': 7, 'Gusht': 8, 'Shtator': 9, 'Tetor': 10, 'Nëntor': 11, 'Dhjetor': 12
            }
            # Split the date string
            day, month, year, time = date_str.replace(',', '').split()
            # Convert month name to number
            month_num = albanian_months[month]
            # Parse the date
            dt = datetime.strptime(f"{day} {month_num} {year} {time}", "%d %m %Y %H:%M")
            # Set the timezone to Tirana (Albania's timezone)
            albania_tz = pytz.timezone('Europe/Tirane')
            dt = albania_tz.localize(dt)
            # Convert to UTC
            dt_utc = dt.astimezone(pytz.UTC)
            # Convert to Unix timestamp
            return int(dt_utc.timestamp())
        return None
