# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
from ..helper import format_date, sq_feet_to_meters, extract_number_only, format_date, remove_white_spaces
from datetime import datetime
import json
import re


class PacificedgepropertiesltdPyspiderCanadaEnSpider(scrapy.Spider):
    name = "PacificEdgePropertiesLtd_PySpider_canada_en"
    start_urls = ['https://api.theliftsystem.com/v2/search?auth_token=sswpREkUtyeYjeoahA2i&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_sqft=0&max_sqft=10000&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&client_id=695&order=max_rate+ASC%2C+min_rate+ASC%2C+min_bed+ASC%2C+max_bath+ASC&min_rate=1700&max_rate=2100&limit=50&count=false']
    allowed_domains = ["pacificedgeproperties.ca", "theliftsystem.com"]
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
        listings = json.loads(response.body)
        for listing in listings:
            yield scrapy.Request(listing['permalink'], callback=self.populate_item, meta={ **listing })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        data = response.meta
        amenities = response.css("span.amenity::text").getall()
        sqaure_feet = response.css("p.sq-ft::text").getall()
        for index, _ in enumerate(response.css("div.table").getall()):
            external_id = str(data['id'])
            title = remove_white_spaces(response.css("h2.sub-heading::text").get())
            description = ''.join(response.css(f"div.suite:nth-child({index + 1}) div.suite-description > p::text").getall())
            city = data['address']['city']
            zipcode = data['address']['postal_code']
            address = data['address']['address']
            latitude = data['geocode']['latitude']
            longitude = data['geocode']['longitude']
            square_meters = None
            if len(sqaure_feet) > index:
                square_meters = int((extract_number_only(sqaure_feet[index]))) if extract_number_only(sqaure_feet[index])!=0 else None
            room_count = None
            if int(extract_number_only(response.css(f"div.suite:nth-child({index + 1}) div.type-name::text").getall())) != 0:
                room_count = int(extract_number_only(response.css(f"div.suite:nth-child({index + 1}) div.type-name::text").getall()))
            else:
                room_count = 1
            property_type = 'apartment' if room_count > 1 else 'studio'
            pets_allowed = None if data['pet_friendly'] == 'n/a' else data['pet_friendly']
            bathroom_count = None
            parking = None
            balcony = None
            dishwasher = None
            for amenity in amenities:
                if amenity.lower().find('balcony') or amenity.lower().find('balconies'):
                    balcony = True
                if amenity.lower().find('dishwasher'):
                    dishwasher = True
                if amenity.lower().find('parking'):
                    parking = True
            images = response.css(f"div.suite:nth-child({index+1}) a.suite-photo::attr(href)").getall()
            if len(images) == 0:
                images = response.css("div.cover::attr(data-src2x)").getall()
            rent = int(extract_number_only(response.css(f"div.suite:nth-child({index+1}) div.rate-value").getall()))
            available_date = response.css(f"div.suite:nth-child({1+index}) span.date::text").get()
            available_date = format_date(available_date, '%b %d, %Y') if available_date else None

            landlord_name = response.css("div.contact-person >  div.name::text").getall() if len(response.css("div.contact-person >  div.name::text").getall()) > 0 else data['client']['name']
            landlord_number = response.css("div.phone-number > a::text").getall() if len(response.css("div.phone-number > a::text").getall()) > 0 else data['client']['phone']
            landlord_email = data['client']['email']
            
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", response.url + f'#{index+1}') # String
            item_loader.add_value("external_source", self.external_source) # String
            item_loader.add_value("position", self.position) # Int

            item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", latitude) # String
            item_loader.add_value("longitude", longitude) # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            #item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            #item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
