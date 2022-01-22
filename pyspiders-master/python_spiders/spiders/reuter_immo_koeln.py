# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup
import datetime
import dateparser



class ReuterImmoKoelnSpider(scrapy.Spider):
    name = "reuter_immo_koeln"
    start_urls = ['https://www.reuter-immobilien-koeln.de/advanced-search/?filter_search_action%5B%5D=zur-miete&filter_search_type%5B%5D=&advanced_city=&advanced_area=&advanced_rooms=&advanced_bath=&price_low=&price_max=&submit=SUCHE']
    allowed_domains = ["reuter-immobilien-koeln.de"]
    country = 'Germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
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
        objects = response.css('#listing_ajax_container .property_listing')
        for object in objects:
            property_url = object.css(' h4 a::attr(href)')[0].extract()
            title = object.css(' h4 a::text')[0].extract().strip()
            rent = object.css(' .listing_unit_price_wrapper::text')[0].extract()
            rent = int(''.join(x for x in rent if x.isdigit()))
            room_count = object.css(' .inforoom::text')[0].extract()
            bathroom_count = object.css(' .infobath::text')[0].extract()
            square_meters = object.css(' .infosize::text')[0].extract()
            square_meters = square_meters.split(' ')[0]

            yield Request(url=property_url, callback=self.populate_item,
                          meta={'rent': rent, 'title': title, 'room_count': room_count,
                                'bathroom_count': bathroom_count, 'square_meters': square_meters})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rent = response.meta['rent']
        title = response.meta['title']
        room_count = response.meta['room_count']
        bathroom_count = response.meta['bathroom_count']
        square_meters = response.meta['square_meters']

        if 'DACHATELIER' in title:
            property_type = 'studio'
        else:
            property_type = 'apartment'

        address = response.css('.notice_area [rel="tag"] ::text').extract()
        address = ', '.join(address)
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)

        description = response.css('#property_description p ::text').extract()
        description = ' '.join(description)
        description = description_cleaner(description)

        list = response.css('.col-md-4 ::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)
        deposit = None
        if 'Kaution: ' in list:
            deposit = list.split('Kaution: ')[1].split(' ')[0]
            deposit = int(''.join(x for x in deposit if x.isdigit()))
        external_id = None
        if 'Objektnummer: ' in list:
            external_id = list.split('Objektnummer: ')[1].split(' ')[0]
        floor = None
        if 'Etage: ' in list:
            floor = list.split('Etage: ')[1].split(' ')[0]
        energy_label = None
        if 'energieeffizienzklasse ist ' in list:
            energy_label = list.split('energieeffizienzklasse ist  ')[1].split('.')[0]

        available_date = None
        if 'Verfügbar ab: ' in list:
            available_date = list.split('Verfügbar ab: ')[1].split(' ')[0]
            try:
                import datetime
                import dateparser
                available_date = available_date.strip()
                available_date = dateparser.parse(available_date)
                available_date = available_date.strftime("%Y-%m-%d")
            except:
                available_date = None

        parking = None
        if 'stellplatz' in description.lower() or 'stellplatz' in list.lower():
            parking = True
        washing_machine = None
        if 'waschmasch' in description.lower() or 'waschmasch' in list.lower():
            washing_machine = True
        dishwasher = None
        if 'geschirr' in description.lower() or 'geschirr' in list.lower():
            dishwasher = True
        terrace = None
        if 'terras' in description.lower() or 'terras' in list.lower():
            terrace = True
        elevator = None
        if 'aufzug' in description.lower() or 'aufzug' in list.lower():
            elevator = True
        balcony = None
        if 'balkon' in description.lower() or 'balkon' in list.lower() or 'balkon' in title.lower():
            balcony = True
        furnished = None
        if 'renoviert' in description.lower() or 'renoviert' in list.lower() or 'MÖBLIERTES ' in title:
            furnished = True
        if int(rent) <= 0 and int(rent) > 40000:
            return

        images = response.css('.item::attr(style)').extract()
        images = [image.split("background-image:url(")[1].split(")")[0] for image in images]

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
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
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Reuter Immobilien GmbH') # String
        item_loader.add_value("landlord_phone", '0221 2570101') # String
        item_loader.add_value("landlord_email", 'info@reuterimmobilienkoeln.de') # String

        self.position += 1
        yield item_loader.load_item()
