# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_number_only, sq_feet_to_meters


class RoyalyorkapartmentsPyspiderCanadaEnSpider(scrapy.Spider):
    name = "RoyalYorkApartments_PySpider_canada_en"
    start_urls = ['https://www.royalyorkapartments.com/royal-york-properties', 'https://www.royalyorkapartments.com/gta-properties']
    allowed_domains = ["royalyorkapartments.com"]
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
        listings = response.css("a.details-button::attr(href)").getall()
        for listing in listings:
            yield scrapy.Request('https://www.royalyorkapartments.com' + listing, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        rents = response.css("div.suite-rate > span::text").getall()
        sqaure_feet = response.css("div.suite-sqft > span.value::text").getall()
        # examples rents = ['from', '$1300', '$1350', '/mo', 'from', '$1650', '$1750', '/mo', 'from', '$2200', '/mo', 'from', '$2400', '/mo']
        mo_index_in_rents = [i for i in range(len(rents)) if rents[i] == '/mo']
        for index, unit in enumerate(response.css("div.suite-type::text").getall()):
            if response.css("div.suite-availability").getall()[index].find('Not Available') != -1:
                continue
            title = unit
            description = response.css("meta[name='description']::attr(content)").get()
            
            latitude = response.css("div.neighborhood.loading-map-screen::attr(data-latitude)").get()
            longitude = response.css("div.neighborhood.loading-map-screen::attr(data-longitude)").get()
            zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
            square_meters = None
            if len(sqaure_feet) > index:
                square_meters = sq_feet_to_meters(sqaure_feet[index]) if sqaure_feet[index].isnumeric() else None
            room_count = None
            if title[0].isnumeric():
                room_count = int(title[0])
            else:
                room_count = 1
            property_type = 'apartment' if room_count > 1 else 'studio'
            bathroom_count = response.css("div.suite-bath span.value::text").getall()[index]
            parking = True if len(response.css("section.parking h2").getall())!=0 else None
            amenities = response.css("section.widget.amenities li::text").getall()
            parking = None
            balcony = None
            dishwasher = None
            elevator = None
            for amenity in amenities:
                if amenity.lower().find('balcony') or amenity.lower().find('balconies'):
                    balcony = True
                if amenity.lower().find('dishwasher'):
                    dishwasher = True
                if amenity.lower().find('parking'):
                    parking = True
                if amenity.lower().find('elevator'):
                    elevator = True
            images = response.css("div.image img::attr(src)").getall()
            rent = extract_number_only(rents[mo_index_in_rents[index]-1])
            landlord_name = "Royal York Apartment Group"
            landlord_number = response.css("div.call-phone h2::text").get()
            
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", response.url + f'#{index+1}') # String
            item_loader.add_value("external_source", self.external_source) # String
            item_loader.add_value("position", self.position) # Int

            #item_loader.add_value("external_id", external_id) # String
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
            # item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
