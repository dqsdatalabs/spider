# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup


class ImmoGallerySpider(scrapy.Spider):
    name = "immo_gallery"
    start_urls = ['https://immobilien-gallery.de/ff/immobilien/?schema=&price=&ffpage=1&sort=date',
                  'https://immobilien-gallery.de/ff/immobilien/?schema=&price=&ffpage=2&sort=date']
    allowed_domains = ["immobilien-gallery.de"]
    country = 'Germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
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
        objects = response.css('.FFestateview-default-overview-estate')
        for object in objects:
            property_url = object.css(' a::attr(href)')[0].extract()
            rent = object.css('.ff-color-primary > div > span ::text')[0].extract()
            rent = rent.split(',')[0]
            rent = int(''.join(x for x in rent if x.isdigit()))
            rented = object.css('.ff-color-primary > div > div > span::text')[0].extract()
            if 'Miete' not in rented:
                continue
            external_id = object.css('.identifier span+ span::text')[0].extract()
            title = object.css('.FFestateview-default-overview-estate-type ::text')[0].extract()
            title = remove_white_spaces(title)
            if 'haus' in title.lower():
                property_type = 'house'
            elif 'wohnung' in title.lower():
                property_type = 'apartment'
            else:
                continue
            address = object.css('.FFestateview-default-overview-estate-addresses ::text')[0].extract()
            address = remove_white_spaces(address)
            square_meters = object.css('.livingarea span+ span::text')[0].extract()
            room_count = 1
            try:
                room_count = object.css('.rooms span+ span::text')[0].extract()
                if '.' in room_count:
                    room_count = int(room_count[0]) + 1
            except:
                pass
            yield Request(url=property_url, callback=self.populate_item,
                          meta={'rent': rent, 'external_id': external_id, 'room_count': room_count,
                                'square_meters': square_meters, 'property_type': property_type, 'address': address})


    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rent = response.meta['rent']
        address = response.meta['address']
        external_id = response.meta['external_id']
        room_count = response.meta['room_count']
        square_meters = response.meta['square_meters']
        property_type = response.meta['property_type']

        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)

        title = response.css('.title ::text').extract()
        description = response.css('#ff-default p ::text').extract()
        description = ' '.join(description)
        description = description_cleaner(description)

        list = response.css('.FFestateview-default-details-content-details span ::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)
        energy_label = None
        try:
            energy_label = response.css('.FFestateview-default-details-content-energyUsage li:nth-child(3) span+ span ::text')[0].extract()
        except:
            pass

        list3 = response.css('.FFestateview-default-details-content-equipments span ::text').extract()
        list3 = ' '.join(list3)
        list3 = remove_white_spaces(list3)

        utilities = None
        if 'Nebenkosten' in list:
            utilities = list.split('Nebenkosten ')[1].split(',')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))
        floor = None
        if ' Etage ' in list:
            floor = list.split(' Etage ')[1].split(' ')[0]

        parking = None
        if 'stellplatz' in description.lower() or 'stellplatz' in list.lower() or 'stellplatz' in list3.lower():
            parking = True
        washing_machine = None
        if 'waschmasch' in description.lower() or 'waschmasch' in list.lower() or 'waschmasch' in list3.lower():
            washing_machine = True
        dishwasher = None
        if 'geschirr' in description.lower() or 'geschirr' in list.lower() or 'geschirr' in list3.lower():
            dishwasher = True
        terrace = None
        if 'terrase' in description.lower() or 'terrase' in list.lower() or 'terrase' in list3.lower():
            terrace = True
        elevator = None
        if 'aufzug' in description.lower() or 'aufzug' in list.lower() or 'aufzug' in list3.lower():
            elevator = True
        balcony = None
        if 'balkon' in description.lower() or 'balkon' in list.lower() or 'balkon' in list3.lower():
            balcony = True
        furnished = None
        if 'renoviert' in description.lower() or 'renoviert' in list.lower() or 'renoviert' in list3.lower():
            furnished = True

        images = response.css('.FFestateview-default-details-main-image a::attr(href)').extract()
        if images == []:
            images = [response.css('.FFestateview-default-details-main-image img::attr(src)')[0].extract()]
        if rent <= 0 and rent > 40000:
            return
        landlord_number = response.css('.FFestateview-default-details-agent a::attr(href)')[0].extract()
        if 'tel:' in landlord_number:
            landlord_number = landlord_number.replace('tel:','')
        else:
            landlord_number = '+49 234 610 655 00'
        landlord_name = 'Immobilien Gallery'
        try:
            landlord_name = response.css('.FFestateview-default-details-agent-name span::text')[0].extract().strip()
        except:
            pass

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", 1) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", 'info@immobilien-gallery.de') # String

        self.position += 1
        yield item_loader.load_item()
