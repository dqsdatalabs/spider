import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json

class icivarazze_it_PySpider_italySpider(scrapy.Spider):
    name = 'icivarazze_it'
    allowed_domains = ['icivarazze.it']
    start_urls = [
        'https://icivarazze.it/immobiliaffitto.php'
        ]
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'


    def parse(self, response):  #page_follower
        urls = response.css("body > section.property-section.sec-ptb-120.clearfix > div > div.row > div > div > a::attr(href)").extract()
        for url in urls:
            url = "https://icivarazze.it/" + url
            yield Request(url=url,
            callback = self.parse_property)

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        rent = response.css(".price-text::text").get().strip()
        if "€" in rent:

            title = response.css(".big-title::text").get()
            external_id = title.split("Rif. ")[1].split(" ")[0]
            property_type = title.split('-')[1]
            if "Appartamento" in property_type:
                property_type = 'apartment'

            description = response.css("#property-details-section b::text").get()

            square_meters = int(response.css(".schedabold:nth-child(5)::text").get())

            info = response.css(".schedabold::text").extract()
            room_count = int(info[1])
            bathroom_count = int(info[9])
            elevator = None
            balcony = None
            terrace = None
            if 'no' in info[3]:
                elevator = False
            if 'no' in info[5]:
                balcony = False
            if 'no' in info[6]:
                terrace = False


            rent = int(rent.replace("€","").split(",")[0])
            
            image_all = response.css("::attr(src)").extract()
            images = []
            for i in range(len(image_all)):
                if "https://icivarazze.it/fotoimmobili/" in image_all[i]:
                    images.append(image_all[i])
            external_images_count = len(images)
            city = response.css("#property-details-section .ul-li li::text").get().replace(" ","")

            item_loader.add_value('external_link', response.url)
            item_loader.add_value('external_id',external_id)        
            item_loader.add_value('external_source', self.external_source)
            item_loader.add_value('title',title)
            item_loader.add_value('description',description)
            item_loader.add_value('city',city)
            item_loader.add_value('property_type',property_type)
            item_loader.add_value('square_meters',square_meters)
            item_loader.add_value('room_count',room_count)
            item_loader.add_value('bathroom_count',bathroom_count)
            item_loader.add_value('images',images)
            item_loader.add_value('external_images_count',external_images_count)
            item_loader.add_value('rent',rent)
            item_loader.add_value('currency','EUR')
            item_loader.add_value('elevator',elevator)
            item_loader.add_value('balcony',balcony)
            item_loader.add_value('terrace',terrace)
            item_loader.add_value('landlord_name','icivarazze')
            item_loader.add_value('landlord_phone','335 80 11 431')
            item_loader.add_value('landlord_email','349 28 27 621')

            # item_loader.add_value(,)
            yield item_loader.load_item()
