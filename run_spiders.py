from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from newsify.spiders.lapsi_spider import LapsiSpider
from newsify.spiders.pamfleti_spider import PamfletiSpider
from newsify.spiders.syri_spider import SyriSpider

class SpiderOutput:
    def __init__(self):
        self.items = []

    def item_scraped(self, item):
        self.items.append(item)

def run_spiders():
    process = CrawlerProcess(get_project_settings())
    
    spiders = [
        ('Lapsi', LapsiSpider),
        ('Pamfleti', PamfletiSpider),
        ('Syri', SyriSpider)
    ]
    
    results = {}
    for name, spider_class in spiders:
        print(f"Adding {name} spider to the process...")
        output = SpiderOutput()
        crawler = process.create_crawler(spider_class)
        crawler.signals.connect(output.item_scraped, signal=signals.item_scraped)
        process.crawl(crawler)
        results[name] = output

    print("Starting the crawling process...")
    process.start()

if __name__ == '__main__':
    run_spiders()
