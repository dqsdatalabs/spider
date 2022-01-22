# -*- coding: utf-8 -*-
# Author: Mohamed Helmy
import scrapy
from ..loaders import ListingLoader
import re

class RevaSpider(scrapy.Spider):
    name = "ecentralliving"
    start_urls = ['https://www.ecentralliving.com/rental-suites']
    #allowed_domains = "https://ecentralliving.com"
    country = 'ca' 
    locale = 'en' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response):
        script = response.xpath('.//div[@class="floorplan-hold list-view"]').extract_first()
        Ids = re.findall(r'FloorplanId:\'(\d{7})\'', script)
        for i in Ids:
            yield scrapy.Request(self.start_urls[0] + "/" + i, callback=self.populate_items)
    
    # 3. SCRAPING level 3
    def populate_items(self, response):
        amenities = response.xpath('.//div[@class="row ameninites mb-0 no-gutters"]/div/div/text()').extract() + response.xpath('.//div[@class="amenities-more sf-more"]/div/div/div/text()').extract()
        price = response.xpath('.//h4[@class="suite-price"]/text()').extract_first().replace(",", "")
        rent = re.findall(r'\d+', price)[0]
        bedrooms = response.xpath('.//div[@class="bed  detail-bx"]/text()').extract_first()
        pet_friendly = False
        washer = False
        balcony = False
        images = response.xpath('.//div[@class="floor-image mb-2"]/img/@src').extract_first()
        sqm = int(response.xpath('.//div[@class="size col-12 detail-bx"]/text()').extract_first().replace(",", ""))
        bath_count = int(response.xpath('.//div[@class="bath  detail-bx"]/text()').extract_first())
        property_type = "apartment"
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("external_source", self.external_source) 
        item_loader.add_value("position", self.position)
        item_loader.add_value("title", response.xpath('.//h1[@class="name-title"]/text()').extract_first())
        
        if (bedrooms == '0'):
            property_type = "studio"
            bedrooms = 1
          
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", sqm)
        item_loader.add_value("room_count", int(bedrooms))
        item_loader.add_value("bathroom_count", bath_count)
        
        for amenity in amenities:
            if ('Pet Friendly' in amenity):
                pet_friendly = True
            if ('washer' in amenity or 'dryer' in amenity):
                washer = True
            if ('Balcony' in amenity):
                balcony = True
        
        item_loader.add_value("pets_allowed", pet_friendly)        
        item_loader.add_value('washing_machine', washer)
        item_loader.add_value('balcony', balcony)
        item_loader.add_value("floor_plan_images", images)
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "CAD") 

        item_loader.add_value("landlord_name", "ecentralliving") 
        item_loader.add_value("landlord_phone", "(647) 846-7620") 
        item_loader.add_value("landlord_email", "ecentral@rhapsodyliving.ca") 

        self.position += 1
        yield item_loader.load_item()
