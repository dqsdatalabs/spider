import scrapy


class ImmobilienDietlSpider(scrapy.Spider):
    name = 'immobilien_dietl'
    allowed_domains = ['https://www.immobilien-dietl.de/mieten']
    start_urls = ['http://https://www.immobilien-dietl.de/mieten/']

    def parse(self, response):
        pass
