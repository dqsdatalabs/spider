# -*- coding: utf-8 -*-
# Author: Adham Mansour
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class VersuslivingComSpider(scrapy.Spider):
    name = 'versusliving_com'
    allowed_domains = ['versusliving.com']
    start_urls = ['https://www.versusliving.com/floorplans']  # https not http
    country = 'canada'
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
    def parse(self, response, **kwargs):
        rentals = response.css('.col-lg-4')
        for rental in rentals:
            external_link = rental.css('.track-apply::attr(href)').extract_first()
            yield Request(url='https://www.versusliving.com'+external_link,
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.css('h1::text').extract_first()
        latitude = "51.043852"
        longitude = '-114.082589'
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        property_type = 'apartment'
        row = response.css('.px-3::text').extract()
        pets_allowed = None
        for element in row:
            if 'Sq' in element:
                square_meters = extract_number_only(element)
            elif 'Bath' in element:
                bathroom_count = extract_number_only(element)
            elif 'Bed' in element:
                room_count = extract_number_only(element)
            elif 'Pet' in element:
                pets_allowed = True

        amenities = ' '.join(response.css('.my-4 .col-sm-6::text').extract())
        amenities = amenities.lower()


        floor_plan_images = response.css('.card-img-top::attr(src)').extract()

        parking = None
        if 'parking' in amenities:
            parking = True


        elevator = None
        if 'elevator' in amenities:
            elevator = True


        balcony = None
        if 'balcon' in amenities:
            balcony = True


        terrace = None
        if 'terrace' in amenities:
            terrace = True

        swimming_pool = None
        if 'pool' in amenities:
            swimming_pool = True


        washing_machine = None
        if 'washer' in amenities:
            washing_machine = True


        dishwasher = None
        if 'dishwasher' in amenities:
            dishwasher = True

        apartments = response.css('#availApts .text-center')
        counter = 1
        for apartment in apartments:
            item_loader = ListingLoader(response=response)
            rent = int(extract_number_only(extract_number_only(apartment.css('.text-muted span::text').extract_first())))
            external_id = apartment.css('.card-title span::text').extract_first()

            # # MetaData
            item_loader.add_value("external_link", response.url+'#'+str(counter))  # String
            item_loader.add_value("external_source", self.external_source)  # String

            item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title) # String
            # item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", latitude) # String
            item_loader.add_value("longitude", longitude) # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            # item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            # item_loader.add_value("images", images) # Array
            # item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", 'versus living') # String
            item_loader.add_value("landlord_phone", '(833) 873-9458') # String
            # item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            counter +=1
            yield item_loader.load_item()
