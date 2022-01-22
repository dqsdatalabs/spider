# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import sq_feet_to_meters
import json


class EkortpmPyspiderCanadaEnSpider(scrapy.Spider):
    name = "EkortPM_PySpider_canada_en"
    start_urls = ['https://api.theliftsystem.com/v2/search?locale=en&client_id=242&auth_token=sswpREkUtyeYjeoahA2i&city_id=2167&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=1800&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=229%2C3144%2C1410%2C2167&pet_friendly=&offset=0&count=false']
    allowed_domains = ["theliftsystem.com", "ekortpm.com"]
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
            if listing['availability_status']:
                yield scrapy.Request(listing['permalink'], callback=self.populate_item, meta={ **listing })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        data = response.meta
        amenities = response.css("div.amenity-holder::text").getall()
        sqaure_feet = response.css("div.suite-sqft.cell > span.value::text").getall()
        for index, unit in enumerate(response.css("script[type='application/ld+json']::text").getall()):
            unit = json.loads(unit)
            if int(unit['price']) == 0:
                continue
            
            external_id = str(data['id'])
            title = unit['name']
            description = response.css("#content > div > div > div > p::text").get()
            city = data['address']['city']
            zipcode = data['address']['postal_code']
            address = data['address']['address']
            latitude = data['geocode']['latitude']
            longitude = data['geocode']['longitude']
            property_type = 'apartment'
            square_meters = None
            if len(sqaure_feet) > index:
                square_meters = sq_feet_to_meters(sqaure_feet[index]) if sqaure_feet[index].isnumeric() else None
            room_count = None
            if unit['name'][0].isnumeric():
                room_count = int(unit['name'][0])
            pets_allowed = data['pet_friendly']
            bathroom_count = int(response.css("div.suite-bath.cell > span.value::text").getall()[index])
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
            images = [data['thumbnail_path']]
            rent = int(unit['price'])
            landlord_name = data['client']['name']
            landlord_number = data['client']['phone']
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
            #item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
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
