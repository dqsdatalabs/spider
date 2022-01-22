import scrapy


class SaareImmobilienSpider(scrapy.Spider):
    name = 'saare_immobilien'
    allowed_domains = ['https://www.saare-immobilien.de/immobilien_angebote.html']
    start_urls = ['http://https://www.saare-immobilien.de/immobilien_angebote.html/']

    def parse(self, response):
        pass
