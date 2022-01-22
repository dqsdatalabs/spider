# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..helper import *
from ..loaders import ListingLoader


class RevaSpider(scrapy.Spider):
    name = "mipprental"
    start_urls = ['https://mipprental.com/c0167']
    allowed_domains = ["mipprental.com"]
    country = 'ca' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        domain = "https://mipprental.com/"
        for url in response.xpath(".//a[@class='img-container']/@href").extract():
            yield scrapy.Request(domain+url, callback=self.populate_item)


    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.xpath(".//div[@class='title-box container hidden-xs hidden-sm']/h1/text()").extract_first()
        address = response.xpath(".//div[@class='location']/text()").extract_first()
        square_meters = int(extract_number_only(response.xpath(".//div[@class='highlight-details-box']/div/span/text()").extract_first()))
        room_count = int(extract_number_only(response.xpath(".//div[@class='highlight-details-box']/div/span/text()").extract()[1]))
        bathroom_count = int(extract_number_only(response.xpath(".//div[@class='value']/text()").extract()[4]))
        listing_id = response.xpath(".//div[@class='value']/text()").extract()[5]
        property_type = response.xpath(".//div[@class='value']/text()").extract()[7]
        rent = int(extract_number_only(response.xpath(".//div[@class='price']/text()").extract_first()))
        description = response.xpath(".//div[@class='b-sec']/span/text()").extract_first()
        amenities = response.xpath(".//li[@class='col-sm-6 col-lg-4 active']/text()").extract()
        washer = None
        dishwasher = None
        balcony = None
        parking = None
        furnished = None
        pets_allowed = None
        floor_plan_img = response.xpath(".//div[@class='box-content']/span/text()").extract_first()
        images = response.xpath(".//div[@class='img-container']/@data-bg-img").extract()
        lat, long = extract_location_from_address(address)
        zipCode, city, stub = extract_location_from_coordinates(lat, long)
        available_date = None
        if(len(response.xpath(".//div[@class='value']/text()").extract()) >= 9):
            if ("2022" in response.xpath(".//div[@class='value']/text()").extract()[8]):
                available_date = format_date(response.xpath(".//div[@class='value']/text()").extract()[8])
        if ("No floor plan" in floor_plan_img):
            floor_plan_img = None
        
        for amenity in amenities:
            if (amenity == 'Dishwasher'):
                dishwasher = True
            if(amenity == "Laundry" or "Washer" in amenity):
                washer = True
            if("parking" in amenity):
                parking = True
            if(amenity == "Pets allowed"):
                pets_allowed = True
        
        property_type = property_type.lower()
        if ("apartment" in property_type or "other" in property_type):
            property_type = "apartment"
        elif ("house" in property_type or "plex" in property_type):
            property_type = "house"
        else:
            property_type = 'apartment'
        
        if rent/1000 > 100 and rent/1000<20000:
            item_loader = ListingLoader(response=response)
            # # MetaData
            item_loader.add_value("external_link", response.url) # String
            item_loader.add_value("external_source", self.external_source) # String

            item_loader.add_value("external_id", listing_id) # String
            item_loader.add_value("position", self.position) # Int
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipCode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", str(lat)) # String
            item_loader.add_value("longitude", str(long)) # String
            #item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count/10) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            #item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washer) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_img) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent/1000) # Int
            #item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            #item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", "Mipprental") # String
            item_loader.add_value("landlord_phone", "519-280-3241") # String
            item_loader.add_value("landlord_email", "admin@jdnpropertys.com") # String

            self.position += 1
            yield item_loader.load_item()
