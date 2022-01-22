# -*- coding: utf-8 -*-
# Author: Mohamed Helmy

import scrapy
from ..loaders import ListingLoader
from ..helper import *

class RevaSpider(scrapy.Spider):
    name = "h2hpm"
    start_urls = ['https://www.h2hpm.com/properties-search/?']
    allowed_domains = ["h2hpm.com"]
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
        for uri in response.xpath(".//a[@class='page-numbers']/@href").extract():
            yield scrapy.Request(uri, callback=self.pagination)

    def pagination(self, response):
        for property_link in response.xpath(".//a[@class='btn-default']/@href").extract():
            if ('commercial' not in property_link):
                yield scrapy.Request(property_link, callback=self.populate_item)
    
    # 3. SCRAPING level 3
    def populate_item(self, response):
        #print(response.url)
        title = response.xpath('.//h1[@class="entry-title single-property-title"]/text()').extract_first()
        labels = response.xpath(".//span[@class='meta-item-label']/text()").extract()[0:7]
        values = response.xpath(".//span[@class='meta-item-value']/text()").extract()[0:7]
        label_value_dic = dict(zip(labels, values))
        
        square_meters = label_value_dic.get("Area")
        property_id = label_value_dic.get("Property ID")
        room_count = label_value_dic.get("Bedrooms")
        bathrooms = label_value_dic.get("Bathrooms")
        property_type = None
        description = ''.join(response.xpath('.//div[@class="property-content"]/p/text()').extract())
        rent = str(extract_number_only(response.xpath('.//span[@class="single-property-price price"]/text()').extract_first())).replace(".","")
        parking = None
        washer = None
        dishwasher = None
        elevator = None
        pets_allowed= None
        amenities = response.xpath('.//ul[@class="property-features-list clearfix"]/li/a/text()').extract()
        images = response.xpath('.//ul[@class="slides"]/li/a/img/@src').extract()
        landlord_name = "Home2Home Properties"
        landlord_phone = "519 890 5245"
        description += ''.join(response.xpath('.//div[@class="property-content"]/p/span/text()').extract())
        rent = int(rent)
        
        if (label_value_dic.get("Location") != None):
            long, lat = extract_location_from_address(label_value_dic.get("Location"))
        elif (title != None):
            long, lat = extract_location_from_address(title)
        zip_code, city, address = extract_location_from_coordinates(long, lat)
        
        if(label_value_dic.get("Type")!= None):
            property_type = "apartment"
        if(description != None):
            if (property_type == None and "house" in description):
                property_type = "house"
            elif("apartment" in description):
                property_type = "apartment"
        for amenity in amenities:
            if ("Parking" in amenity or "parking" in description):
                parking = True
            if ("Laundry" in amenity or "laundry" in description):
                washer = True
            if ("Dishwasher" in amenity):
                dishwasher = True
            if ("Elevator" in amenity):
                elevator = True
            if ("Pets" in amenity):
                pets_allowed = True
            
       
         
        item_loader = ListingLoader(response=response)
        if(rent != 0):
            # # MetaData
            item_loader.add_value("external_link", response.url) # String
            item_loader.add_value("external_source", self.external_source) # String

            item_loader.add_value("external_id", property_id) # String
            item_loader.add_value("position", self.position) # Int
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zip_code) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", str(lat)) # String
            item_loader.add_value("longitude", str(long)) # String
            #item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathrooms) # Int

            #item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            #item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washer) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            item_loader.add_value("landlord_email", "h2hproperties@gmail.com") # String

            self.position += 1
            yield item_loader.load_item()
