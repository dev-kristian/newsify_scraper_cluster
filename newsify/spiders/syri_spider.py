from .base_spider import BaseNewsSpider
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import pytz
from ..firebase_manager import FirebaseManager

class SyriSpider(BaseNewsSpider):
    name = 'syri'
    start_urls = [
        'https://www.syri.net/politike',
    ]

    def __init__(self, *args, **kwargs):
        super(SyriSpider, self).__init__(*args, **kwargs)
        self.article_count = {url: 0 for url in self.start_urls}
        self.firebase_manager = FirebaseManager()
        self.db = self.firebase_manager.client
        self.url_ledger = self.get_url_ledger()

    def parse(self, response):
        sections = response.css('div.categ-left, div.col-sm-6.col-xs-12.new-style, div.col-md-3.col-sm-6.col-xs-12.news-box.blue, div.col-md-4.col-sm-4.col-xs-12.news-box.blue')
        category = response.url.split('/')[-1]

        for section in sections:
            if self.article_count[response.url] >= self.max_articles:
                continue

            article = section.css('a')
            href = article.css('::attr(href)').get()

            if href not in self.url_ledger.get(category, set()):
                title = article.css('h1::text, h2::text').get()
                
                thumbnail = None
                if section.css('div.categ-lg').get():
                    thumbnail = section.css('div.categ-lg').xpath('@style').re_first(r'url\((.*?)\)')
                else:
                    thumbnail = section.css('div.img-cover, div.categ-sm.img-cover, div.img-holder.img-cover').xpath('@data-original').get()

                # Clean and add prefix to thumbnail URL
                if thumbnail:
                    thumbnail = thumbnail.strip("'")  # Remove single quotes
                    if not thumbnail.startswith('http'):
                        thumbnail = f"https://syri.net{thumbnail}"

                yield response.follow(
                    href,
                    callback=self.parse_article,
                    meta={
                        'article_title': title,
                        'article_url': href,
                        'article_thumbnail': thumbnail,
                        'article_category': category
                    }
                )

                self.article_count[response.url] += 1
            else:
                self.logger.info(f"Skipping already scraped article: {href}")

    def parse_article(self, response):
        content = self.extract_content(response)
        gallery_images = self.get_gallery_images(response)
        content.extend([{'type': 'image', 'content': img_src} for img_src in gallery_images])
        return self.create_article_item(response, content)

    def get_content_elements(self, response):
        return response.css('div.readmore-text-here p')

    def parse_content_element(self, element):
        text_parts = []
        for node in element.xpath('.//text()|.//em/text()'):
            text_parts.append(node.get().strip())
        text = ' '.join(filter(None, text_parts))
        
        image = element.css('img::attr(src)').get()
        iframe = element.css('iframe::attr(src)').get()
        
        if text:
            return {'type': 'paragraph', 'content': text}
        if image:
            return {'type': 'image', 'content': image}
        if iframe:
            return {'type': 'iframe', 'content': iframe}

    def get_article_image(self, response):
        image_url = response.css('div.prime-left.readmore iframe::attr(src), div.prime-left.readmore img::attr(src)').get()
        if image_url and not image_url.startswith('http'):
            return f"https://syri.net{image_url}"
        return image_url

    def get_published_date(self, response):
        time = response.css('span.date strong::text')[0].get()
        date = response.css('span.date strong::text')[1].get()
        
        if time and date:
            # Combine date and time
            datetime_str = f"{date.strip()} {time.strip()}"
            
            # Parse the datetime string
            dt = datetime.strptime(datetime_str, "%d/%m/%Y %H:%M")
            
            # Set the timezone to Tirana (Albania's timezone)
            albania_tz = pytz.timezone('Europe/Tirane')
            dt = albania_tz.localize(dt)
            
            # Convert to UTC
            dt_utc = dt.astimezone(pytz.UTC)
            
            # Convert to Unix timestamp
            return int(dt_utc.timestamp())
        return None

    def get_gallery_images(self, response):
        gallery_images = []
        gallery = response.css('div.fotogaleri')
        if gallery:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            driver = webdriver.Chrome(service=Service('C:/Users/Kristian/Desktop/scrapy/chromedriver-win64/chromedriver.exe'), options=chrome_options)
            driver.get(response.url)
            
            try:
                gallery = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'fotogaleri'))
                )
                image_elements = gallery.find_elements(By.TAG_NAME, 'img')
                for img in image_elements:
                    gallery_images.append(img.get_attribute('src'))
            finally:
                driver.quit()

        return gallery_images
