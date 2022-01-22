# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class TuratiAthenaSpider(Spider):
    name = 'turatiathena_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.https://turatiathena.it"]
    start_urls = ["https://turatiathena.it/affitto/immobili-residenziali/"]

    property_dictionary = {"trilocale": 3, "bilocale": 2, "plurilocale": "MULTI-ROOM", 
                        "MONOLOCALE": "studio", "quadrilocale": 4, "MANSARDA": "ATTIC"}
    
    def parse(self, response):
        
        for url in response.css("article:not(.tag-affittato) li div div div a.fusion-post-slideshow::attr(href)").getall():
            yield Request(url=url, callback=self.populate_item, dont_filter=True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        apartment_data = response.css("span.span-custom-field::text").getall()
        rent = apartment_data[2]
        square_meters = apartment_data[1]

        title = response.css(".entry-title::text").get()
        
        description = response.css(".fusion-builder-column-2 > div:nth-child(1) > div:nth-child(1) > p:nth-child(1)::text").getall()
        if not description:
            description = response.css(".fusion_builder_column_1_1 > div:nth-child(1) > div:nth-child(1) > p:nth-child(1)::text").getall()
        
        description = " ".join(description)
        
        images = response.css("div.gallery a::attr(href)").getall()
        
        property_italian_type = response.css("span.span-custom-field a::text").get()
        if(isinstance(self.property_dictionary[property_italian_type], int)):
            property_type = "Apartment"
            room_count = self.property_dictionary[property_italian_type]

        if(self.property_dictionary[property_italian_type] == "MULTI-ROOM"):
            property_type = "Apartment"
            room_count = self.property_dictionary[property_italian_type]

        if(self.property_dictionary[property_italian_type] == "studio"):
            property_type = "studio"
            room_count = 1

        if(self.property_dictionary[property_italian_type] == "ATTIC"):
            property_type = "room"
            room_count = 1

        landlord_name = "Athena immobili di prestigo"
        landlord_phone = " 02.6554435"
        landlord_email = "info@turatiathena.it"

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("images", images)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
       
        yield item_loader.load_item()
