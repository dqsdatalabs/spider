# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
from functools import partial
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_number_only
import re
import json

class TherealtystorePyspiderCanadaEnSpider(scrapy.Spider):
    name = "TheRealtyStore_PySpider_canada_en"
    start_urls = ['https://therealtystore.ca/for-rent/?wplpage=1']
    allowed_domains = ["therealtystore.ca"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 
    canada_property_mapping = {
        'house': 'house',
        'main': 'apartment',
        'apartment': 'apartment',
        'basement': 'apartment',
        'four plex': 'apartment',
        'condo': 'apartment',
        'duplex': 'apartment',
    }

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        if response.css("li.next > a::attr(href)").get() != '#':
            for listing in  response.css("div.wpl_prp_bot a::attr(href)").getall():
                yield scrapy.Request(listing, callback=self.populate_item)
            yield response.follow(response.css("li.next > a::attr(href)").get(), callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.css("h1.title_text::text").get()
        if title is None:
            return
        property_data_keys = response.css("div.rows.other::text").getall()
        property_data_values = response.css("div.rows.other > span::text").getall()
        features = response.css("div.feature label::text").getall()
        external_id = property_type = room_count = bathroom_count = pets_allowed = rent = parking = balcony =  washing_machine = dishwasher = None
        for index, key in enumerate(property_data_keys):
            if 'property type' in key.lower():
                if not property_data_values[index].lower() in self.canada_property_mapping:
                    return
                property_type = self.canada_property_mapping[property_data_values[index].lower()]
            elif 'id' in key.lower():
                external_id = property_data_values[index]
            elif 'bedroom' in key.lower():
                room_count = property_data_values[index]
            elif 'bathroom' in key.lower() and 'half bathroom' not in key.lower():
                bathroom_count = property_data_values[index]
            elif 'price' in key.lower() and 'price type' not in key.lower():
                rent = int(float(extract_number_only(property_data_values[index], thousand_separator=',', scale_separator='.')))
            elif 'pet' in key.lower():
                pets_allowed = True
        for feature in features:
            if 'parking' in feature.lower():
                parking = True   
            elif 'washing machine' in feature.lower():   
                washing_machine = True
            elif 'balcon' in feature.lower():
                balcony = True
            elif 'dishwasher' in feature.lower():
                dishwasher = True  
        # description = response.css("meta[property='og:description']::attr(content)").get()
        description = response.css("div.et_builder_inner_content.et_pb_gutters3  p::text").getall()
        description = [pragraph for pragraph in description if not any(x in pragraph.lower() for x in ['email', 'facebook', 'website', 'https', 'realty'])]
        location_object = json.loads(re.findall(r"\[(.*?)\]", response.css("script[type='text/javascript']::text").getall()[-1])[0])
        latitude = location_object['googlemap_lt']
        longitude = location_object['googlemap_ln']
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        images = response.css("ul.wpl-gallery-pshow li::attr(data-src)").getall()
        item_loader = ListingLoader(response=response)

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String
        item_loader.add_value("position", self.position) # Int
        
        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        #item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "CAD") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'The Realty Store') # String
        item_loader.add_value("landlord_phone", '403-341-5554') # String
        item_loader.add_value("landlord_email", 'reception@therealtystore.ca') # String

        self.position += 1
        yield item_loader.load_item()
