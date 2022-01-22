# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_number_only
import json
from dateutil.parser import parse
from datetime import datetime


class HarroninvestmentsincPyspiderCanadaEnSpider(scrapy.Spider):
    name = "HarronInvestmentsInc_PySpider_canada_en"
    start_urls = ['https://hrent.ca/rent']
    allowed_domains = ["hrent.ca"]
    country = 'canada' # Fill in the Country's name
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
        link_text = response.css("div.sqs-block-button-container--center > a::text").getall()
        listings = response.css("div.sqs-block-button-container--center > a::attr(href)").getall()
        locations = [json.loads(x)['location'] for x in response.css("div::attr(data-block-json)").getall()]
        contact_info = [x.strip() for x in response.css("p[style='margin-left:40px;white-space:pre-wrap;'] > *::text").getall() if x.strip()!='']
        index = 0
        for indx, listing in enumerate(listings):
            if 'learn' in link_text[indx].strip().lower():
                units = response.css("div.col.sqs-col-6.span-6")[index].css("div.sqs-block-content > h2[style='white-space:pre-wrap;'] > strong::text").getall()
                if len(units) == 0:
                    index += 1
                else:
                    meta = {'units': units, 'location': locations[index], 'contact_info': contact_info[index]}
                    index += 1
                    yield response.follow(listing, callback=self.populate_item, meta=meta)
        

    # 3. SCRAPING level 3
    def populate_item(self, response):
        units = response.meta['units']
        
        location = response.meta['location']
        contact_info = response.meta['contact_info']
        landlord_name = contact_info.split(':')[1].strip()
        landlord_number = contact_info.split(':')[0].split(' ')[0].strip()
        while not len(landlord_name) == 0 and not landlord_number[0].isdigit():
            landlord_number = landlord_number[1:]
        description = response.css("div.sqs-block-content> p::text").getall()
        while len(description) > 0 and not 'manager' in description[-1].lower():
            description = description[:-1]
        description = description[:-1]
        title = response.css("h1::text").get()
        latitude = location["mapLat"]
        longitude = location['mapLng']
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        amenities = response.css("ul[data-rte-list='default'] > li > p::text").getall()
        parking = balcony = dishwasher = pets_allowed = elevator = None
        for amenity in amenities:
            if 'balcon' in amenity.lower():
                balcony = True
            if 'dishwasher' in amenity.lower():
                dishwasher = True
            if 'parking' in amenity.lower():
                parking = True
            if 'pet' in amenity.lower():
                pets_allowed = False if 'no' in amenity.lower() else True
            if 'elevator' in amenity.lower():
                elevator = True
        images = response.css("noscript>img::attr(src)").getall()
        for index, unit in enumerate(units):
            item_loader = ListingLoader(response=response)
            room_count = unit.split('-')[0].strip()[0]
            if room_count.isnumeric():
                room_count = int(room_count)
            else:
                room_count = 1
            property_type = 'apartment' if room_count > 1 else 'studio'
            available_date = unit.split('-')[1].strip()
            if 'available now' in available_date.lower():
                available_date = datetime.now().strftime("%Y-%m-%d")
            else:
                available_date =  parse(available_date).strftime("%Y-%m-%d")
            
            rent = int(float(extract_number_only(unit.split('-')[2].strip())))

            # # MetaData
            item_loader.add_value("external_link", response.url + f'#{index+1}') # String
            item_loader.add_value("external_source", self.external_source) # String

            #item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position) # Int
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
            #item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            #item_loader.add_value("washing_machine", washing_machine) # Boolean
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
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_number) # String
            #item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
