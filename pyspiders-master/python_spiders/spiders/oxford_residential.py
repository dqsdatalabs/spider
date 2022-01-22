# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *


class OxfordResidentialSpider(scrapy.Spider):
    name = "oxford_residential"
    start_urls = ['https://www.oxfordresidential.ca/en-ca/our-apartments']
    allowed_domains = ["oxfordresidential.ca"]
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
        property_urls = response.css('.line-3 .btn-type-2::attr(href)').extract()
        property_urls = ['https://www.oxfordresidential.ca' + x for x in property_urls]
        for property_url in property_urls:
            yield Request(url=property_url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        description = response.css(".highlight p::text")[0].extract()
        title = response.css("h1::text")[0].extract()
        info = response.css(".contacts::text").extract()
        landlord_number = info[1][7:]
        latlng = response.css('script:contains("myLatlng")::text').get()
        coords = latlng.split(" lat: ")[1].split(" };")[0]
        coords = coords.split(", lng: ")
        latitude = coords[0]
        longitude = coords[1]
        zipcode,city,address = extract_location_from_coordinates(longitude,latitude)
        list = response.css("#property-details li::text").extract()
        list = ' '.join(list)
        swimming_pool = None
        if 'Pool' in list:
            swimming_pool = True
        parking = None
        if 'Parking' in list:
            parking = True
        balcony = None
        if 'Balcony' in list or 'Balconies' in list:
            balcony = True
        dishwasher = None
        if 'Dishwasher' in list:
            dishwasher = True
        pets_allowed = None
        if 'Pet-friendly' in list or 'Pet Lounge' in list:
            pets_allowed = True
        terrace = None
        if 'terrace' in list or 'terraces' in list:
            terrace = True
        washing_machine = None
        if 'Washer' in list or 'Washing' in list or 'Laundry' in list:
            washing_machine = True
        elevator = None
        if 'Elevator' in list or 'Elevators' in list:
            elevator = True

        images = response.css("#tab-content-1 img::attr(src)").extract()
        for image in images:
            if " " in image:
                images = {x.replace(' ', '%20') for x in images}
            else:
                pass

        columns = response.css(".text-2::text").extract()
        not_avi = response.css("#property-details > div > main > ul.suites-types-list > li.not-available > div.cell-2 > span > strong::text").extract()
        avi = response.css("#property-details > div > main > ul.suites-types-list > li > div.cell-2 > span > strong::text").extract()
        avi = [x for x in avi if x not in not_avi]

        for i in range(len(columns)):
            item_loader = ListingLoader(response=response)
            external_link = response.url + '#' + str(i+1)
            rent = None
            try:
                rent = response.css(".text-2::text")[i].extract()
                if any(char.isdigit() for char in rent):
                    rent = ''.join(x for x in rent if x.isdigit())
                    rent = int(rent[:-2])
            except:
                return
            floor_plan_images = None
            try:
                floor_plan_images = response.css(".floorplans .btn-type-4::attr(href)")[i].extract()
            except:
                pass
            room_count = avi[i]
            if 'Studio' in room_count:
                property_type = 'studio'
                room_count = 1
            elif 'One' in room_count:
                property_type = 'apartment'
                room_count = 1
            elif 'Two' in room_count:
                property_type = 'apartment'
                room_count = 2
            elif 'Three' in room_count:
                property_type = 'apartment'
                room_count = 3
            elif 'Four' in room_count:
                property_type = 'apartment'
                room_count = 4


            # # MetaData
            item_loader.add_value("external_link", external_link) # String
            item_loader.add_value("external_source", self.external_source) # String

            #item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position) # Int
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
            #item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            #item_loader.add_value("bathroom_count", bathroom_count) # Int

            #item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
            item_loader.add_value("landlord_name", title) # String
            item_loader.add_value("landlord_phone", landlord_number) # String
            #item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
