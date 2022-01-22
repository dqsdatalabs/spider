# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *

class VancouverForLivingSpider(scrapy.Spider):
    name = "vancouver_for_living"
    start_urls = ['https://vancouverforliving.com/furnished-rentals/']
    allowed_domains = ["vancouverforliving.com"]
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
        property_urls = response.css('h3 a::attr(href)').extract()
        for property_url in property_urls:
            yield Request(url=property_url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        location = response.css('script:contains("av_google_map")::text').get()
        longitude = location.split("['long'] = ")[1].split(';')[0]
        latitude = location.split("['lat'] = ")[1].split(';')[0]
        zipcode = location.split("['postcode'] = ")[1].split(';')[0]
        city = location.split("['city'] = ")[1].split(';')[0]
        address = location.split("['address'] = ")[1].split(';')[0]
        zipcode = zipcode.replace('"','')
        city = city.replace('"','')
        address = address.replace('"','')
        title = response.css("#av_section_1 h1::text")[0].extract()
        description = response.css("#av_section_1 p ::text").extract()
        description = ' '.join(description)
        if 'one bedroom' in description.lower() or 'studio' in description.lower():
            property_type = 'studio'
            room_count = 1
        else:
            property_type = 'apartment'
            room_count = None
        bathroom_count = 1
        list = response.css("#av_section_2 li ::text").extract()
        list = ' '.join(list)
        rent = int(list.split('Long term rates:   $')[1].split('/month')[0])
        deposit = rent//2


        water_cost = None
        if 'Hydro/electricity, gas' in list:
            water_cost = int(list.split(', hot water $')[1].split(' Hydro/electricity, gas')[0])


        square_meters = int(list.split('Finished floor area ')[1].split(' sq ft ')[0])

        furnished = None
        if 'furnished' in list.lower():
            furnished = True
        swimming_pool = None
        if 'swimming pool' in list.lower():
            swimming_pool = True
        balcony = None
        if 'balcony' in list.lower():
            balcony = True
        parking = None
        if 'parking' in list.lower():
            parking = True
        washing_machine = None
        if 'washer' in list.lower():
            washing_machine = True

        images = response.css('.avia-slideshow-inner a::attr(href)').extract()
        for image in images:
            if "1030x773" in image:
                images = [x.replace('-1030x773', '') for x in images]
            elif "1030x771" in image:
                images = [x.replace('-1030x771', '') for x in images]
            else:
                pass

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
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
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "CAD") # String

        item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Vancouver for Living') # String
        #item_loader.add_value("landlord_phone", landlord_number) # String
        #item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
