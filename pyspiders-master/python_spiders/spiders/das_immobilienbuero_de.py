# -*- coding: utf-8 -*-
# Author: Adham Mansour
from datetime import datetime
from math import ceil

import dateparser
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class Das_immobilienbueroDeSpider(scrapy.Spider):
    name = 'das_immobilienbuero_de'
    allowed_domains = ['das-immobilienbuero.de']
    start_urls = ['https://www.das-immobilienbuero.de/vermietung/']  # https not http
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    duplicate = {}
    position = 1
    keywords = {
        'pets_allowed': ['Haustiere erlaubt'],
        'furnished': ['m bliert', 'ausstattung'],
        'parking': ['garage', 'Stellplatz' 'parkh user', 'Parkplatz'],
        'elevator': ['fahrstuhl', 'aufzug'],
        'balcony': ['balkon'],
        'terrace': ['terrasse'],
        'swimming_pool': [' baden', 'schwimmen', 'schwimmbad', 'pool', 'Freibad'],
        'washing_machine': ['waschen', 'w scherei', 'waschmaschine', 'waschk che'],
        'dishwasher': ['geschirrspulmaschine', 'geschirrsp ler', ],
        'floor': ['etage'],
        'bedroom': ['Schlafzimmer']
    }
    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('#menueleiste_format~ #menueleiste_format a::attr(href)').extract()
        for rental in rentals:
            yield Request(url='https://www.das-immobilienbuero.de/'+rental,
                          callback=self.populate_item)
        pagination = response.css('.pagination a')
        for page in pagination:
            page_text = page.css('span::text').extract_first()
            if page_text == '>':
                external_link = page.css('::attr(href)').extract_first()
                yield scrapy.Request('https://www.das-immobilienbuero.de/'+external_link, callback=self.parse)
    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        external_id = None # response.css('::text').extract_first()
        title = response.css('.with-images > h4::text').extract_first()
        description =((((' '.join(response.css('.bodytext::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        address = response.css('.address p::text').extract_first()
        latitude = None
        longitude = None
        city = None
        zipcode = None
        list_items = response.css('.overview tr')
        list_items_dict = {}
        for list_item in list_items:
            hd_val = list_item.css('td::text').extract()
            if hd_val !=[]:
                header = hd_val[0]
                value =  hd_val[1]
                list_items_dict[header] = value
        property_type = None  # response.css('::text').extract_first()
        if 'Haustyp' in list_items_dict.keys():
            property_type = list_items_dict['Haustyp']

        if 'wohnung' in property_type.lower():
            property_type = 'apartment'
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            square_meters = None  # METERS #int(response.css('::text').extract_first())
            if 'Wohnfläche ca.' in list_items_dict.keys():
                square_meters = list_items_dict['Wohnfläche ca.']
                square_meters = int(ceil(float(extract_number_only(square_meters))))
            room_count = None  # int(response.css('::text').extract_first())
            if 'Zimmeranzahl' in list_items_dict.keys():
                room_count = list_items_dict['Zimmeranzahl']
                room_count = int(ceil(float(extract_number_only(room_count))))
            bathroom_count = None  # int(response.css('::text').extract_first())

            available_date = None  # response.css('.availability .meta-data::text').extract_first()
            if 'Frei ab' in list_items_dict.keys():
                available_date = list_items_dict['Frei ab']
                available_date = (extract_number_only(available_date))
                if len(str(available_date)) > 2:
                    available_date = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if available_date:
                        if available_date > datetime.now():
                            available_date = available_date.strftime("%Y-%m-%d")
                        else:
                            available_date = None
                    else:
                        available_date =None
                else:
                    available_date= None
            images = response.css('#c52 .image a::attr(href)').extract()
            rent = (response.css('.price p::text').extract_first())
            rent = int(ceil(float(extract_number_only(rent))))
            deposit = None
            if 'Kaution' in list_items_dict.keys():
                deposit = list_items_dict['Kaution']
                deposit = int(ceil(float(extract_number_only(deposit))))
            utilities = None
            if 'Nebenkosten' in list_items_dict.keys():
                utilities = list_items_dict['Nebenkosten']
                utilities = int(ceil(float(extract_number_only(utilities))))

            pets_allowed = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['pets_allowed']):
                pets_allowed = True

            furnished = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['furnished']):
                furnished = True

            parking = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['parking']):
                parking = True

            elevator = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['elevator']):
                elevator = True

            balcony = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['balcony']):
                balcony = True

            terrace = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['terrace']):
                terrace = True

            swimming_pool = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['swimming_pool']):
                swimming_pool = True

            washing_machine = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['washing_machine']):
                washing_machine = True

            dishwasher = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['dishwasher']):
                dishwasher = True

            landlord_name = None
            landlord_email = None
            landlord_phone = None
            description =((((' '.join(response.css('.description .bodytext::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))

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
            item_loader.add_value("latitude", str(latitude)) # String
            item_loader.add_value("longitude", str(longitude)) # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed
            #
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
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR") # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", 'das-immobilienbuero') # String
            item_loader.add_value("landlord_phone", "09131/78750") # String
            # item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
