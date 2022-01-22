# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *


class PremiumRentalsSpider(scrapy.Spider):
    name = "premium_rentals"
    start_urls = ['https://premiumrentals.ca/properties/']
    allowed_domains = ["premiumrentals.ca"]
    country = 'Canada' # Fill in the Country's name
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
        property_urls = response.css('#content a::attr(href)').extract()
        for property_url in property_urls:
            if self.allowed_domains[0] in property_url:
                yield Request(url=property_url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        try:
            title = response.css('.text-shadow-3 h1::text')[0].extract()
        except:
            title = response.css('.text-center .uppercase::text')[0].extract()

        address = None
        longitude = None
        latitude = None
        zipcode = None
        city = None
        try:
            address = response.xpath("//*[contains(text(), 'Address')]").css("::text")[0].extract()
            address = address.split('Address: ')[1]
        except:
            try:
                address = response.css(".col-inner strong::text")[0].extract()
            except:
                pass

        try:
            if address != None:
                longitude, latitude = extract_location_from_address(address)
                zipcode,city,address = extract_location_from_coordinates(longitude,latitude)
                latitude = str(latitude)
                longitude = str(longitude)
            else:
                city = "Edson"
                longitude, latitude = extract_location_from_address(city)
                zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
                latitude = str(latitude)
                longitude = str(longitude)
        except:
            latitude, longitude, zipcode, city = None

        images = response.css('.gallery-col a::attr(href)').extract()
        floor_plans = response.css('noscript img::attr(src)').extract()
        floor_plan_images = list(filter(lambda p: 'Layout' in p or 'Unit' in p, floor_plans))
        description = response.css('.large-7 > .col-inner p::text').extract()
        if description == []:
            description = response.css('.large-8 h4+ p ::text').extract()
        description = ' '.join(description)
        if len(description) < 50:
            description = response.css('p:nth-child(6) ::text')[0].extract()

        property_type = 'apartment'

        offers = response.css('h4::text, em::text').extract()
        offers = ' '.join(offers)
        details = response.css('td::text').extract()
        sqft = filter(lambda square: 'Sq Ft' in square, details)
        sqft = list(sqft)
        details_t = ' '.join(details)
        bathrooms = details_t.split('Bathrooms')[1].split('Parking')[0]
        bathrooms = ''.join(x for x in bathrooms if x.isdigit())
        furnishes = details_t.split('Rental Type')[1].split('Rental Terms')[0].strip()
        furnishes = furnishes.split("Or")
        pets_allowed = None
        if 'pet' in details_t.lower():
            pets_allowed = True
        parking = None
        if 'parking' in details_t.lower():
            parking = True
        elevator = None
        if 'elevator' in details_t.lower():
            elevator = True
        washing_machine = None
        if 'laundry' in details_t.lower():
            washing_machine = True
        dishwasher = None
        if 'dishwasher' in details_t.lower():
            dishwasher = True

        if 'apartments' not in offers.lower():
            index = 0
            rents = []
            while index < len(offers):
                index = offers.find('$', index)
                if index == -1:
                    break
                rents.append(offers[index - 10:index + 6])
                index += 1

        else:
            index = 0
            rents = []
            while index < len(offers):
                index = offers.find('$', index)
                if index == -1:
                    break
                rents.append(offers[index +1:index + 8])
                index += 1

        for i, x in enumerate(rents):
            item_loader = ListingLoader(response=response)
            external_link = response.url + '#' + str(i + 1)
            if 'Bedroom' in x:
                room_count = int(x[0])
                rent = x.split('$')[1].strip()
                if ',' in rent:
                    rent = int(''.join(z for z in rent if z.isdigit()))
                else:
                    rent = int(rent[:-1])
            elif ',' in x:
                room_count = int(x[-1])
                rent = x[:-1]
                rent = int(''.join(z for z in rent if z.isdigit()))
            else:
                room_count = int(x[4])
                rent = int(x[:3])
            square_meters = sqft[i]
            square_meters = int(''.join(x for x in square_meters if x.isdigit()))
            bathroom_count = None
            try:
                bathroom_count = bathrooms[i]
            except:
                pass
            furnished = None
            try:
                if ':' not in furnishes[i]:
                    if 'un' not in furnishes[i].lower():
                        furnished = True
                    else:
                        pass
            except:
                pass

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
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            #item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            #item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
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
            item_loader.add_value("landlord_name", 'Premium Rentals') # String
            item_loader.add_value("landlord_phone", '(780) 712-5907') # String
            #item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
