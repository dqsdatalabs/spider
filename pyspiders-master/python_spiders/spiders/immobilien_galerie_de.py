# -*- coding: utf-8 -*-
# Author: Adham Mansour
import re
from datetime import datetime
from math import ceil

import dateparser
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class immobilien_galerieDeSpider(scrapy.Spider):
    name = 'immobilien_galerie_de'
    allowed_domains = ['immobilien-galerie.de']
    start_urls = [
        'https://www.immobilien-galerie.de/wohnung/alle-orte/',
        'https://www.immobilien-galerie.de/haus/alle-orte/'
    ]  # https not http
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
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
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('.description a::attr(href)').extract()
        for rental in rentals:
            yield Request(url='https://www.immobilien-galerie.de/'+(rental),
                          callback=self.populate_item)
        pagination = response.css('.pagination li a')
        for page in pagination:
            page_text = page.css('span::text').extract_first()
            if page_text == '>':
                next_page = page.css('::attr(href)').extract_first()
                yield scrapy.Request(response.urljoin(next_page), callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css('h1::text').extract_first()
        description = ((((' '.join(response.css('.bodytext::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        address = response.css('.address p::text').extract()
        lat_long = response.css('script:contains("LatLng")').extract_first()
        lat_long = re.findall('center: new google\.maps\.LatLng\((-?\d+\.\d+),(-?\d+\.\d+)\)',lat_long)
        if len(lat_long[0]) ==2:
            latitude = lat_long[0][0]
            longitude = lat_long[0][1]
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        else:
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        list_items = response.css('tr')
        list_items_dict = {}
        for list_item in list_items:
            head_val = list_item.css('td::text').extract()
            if len(head_val) == 2:
                headers = head_val[0]
                values = head_val[1]
                list_items_dict[headers] = values

        property_type = None  # response.css('::text').extract_first()
        if 'Immobilien-Art' in list_items_dict.keys():
            property_type = list_items_dict['Immobilien-Art']
            if 'wohnung' in property_type.lower():
                property_type = 'apartment'
            elif 'haus' in property_type.lower():
                property_type = 'house'
            else:
                property_type = None
        if property_type:
            square_meters = None  # METERS #int(response.css('::text').extract_first())
            if 'Wohnfläche ca.' in list_items_dict.keys():
                square_meters = list_items_dict['Wohnfläche ca.']
                square_meters = int(ceil(float(extract_number_only(square_meters))))
            room_count = None  # int(response.css('::text').extract_first())
            if 'Zimmeranzahl' in list_items_dict.keys():
                room_count = list_items_dict['Zimmeranzahl']
                room_count = int(ceil(float(extract_number_only(room_count))))
            bathroom_count = None  # int(response.css('::text').extract_first())
            if 'Anzahl Badezimmer' in list_items_dict.keys():
                bathroom_count = list_items_dict['Anzahl Badezimmer']
                bathroom_count = int(ceil(float(extract_number_only(bathroom_count))))
            available_date = None  # response.css('.availability .meta-data::text').extract_first()
            if 'Frei ab' in list_items_dict.keys():
                available_date = list_items_dict['Frei ab']
                if len(str(available_date)) > 2 and available_date is not None:
                    available_date = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if available_date:
                        if available_date > datetime.now():
                            available_date = available_date.strftime("%Y-%m-%d")
                        else:
                            available_date = None
                    else:
                        available_date = None
                else:
                    available_date = None
            images = response.css('.image a::attr(href)').extract()
            floor_plan_images = response.css('.image a')
            floor_plan_images = [response.urljoin(i.css('::attr(href)').extract_first()) for i in floor_plan_images if 'grundriss' in (i.css('img::attr(title)').extract_first()).lower()]
            rent = None  # response.css('::text').extract_first()
            if 'Kaltmiete' in list_items_dict.keys():
                rent = list_items_dict['Kaltmiete']
                rent = int(ceil(float(extract_number_only(rent))))
            deposit = None
            if 'Kaution' in list_items_dict.keys():
                deposit = list_items_dict['Kaution']
                deposit = int(ceil(float(extract_number_only(deposit))))
            utilities = None
            if 'Nebenkosten' in list_items_dict.keys():
                utilities = list_items_dict['Nebenkosten']
                utilities = int(ceil(float(extract_number_only(utilities))))
            energy_label = None
            if 'Energieeffizienzklasse' in list_items_dict.keys():
                energy_label = list_items_dict['Energieeffizienzklasse']
            floor = None  # response.css('::text').extract_first()
            if 'Etage' in list_items_dict.keys():
                floor = list_items_dict['Etage']
            pets_allowed = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['pets_allowed']):
                pets_allowed = True

            furnished = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['furnished']):
                furnished = True

            parking = None
            if 'Anzahl Stellplätze' in list_items_dict.keys():
                parking = list_items_dict['Anzahl Stellplätze']
                parking = int(ceil(float(extract_number_only(parking))))
                if parking > 0:
                    parking = True

            elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['parking']):
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

            landlord_name = 'Mrs. Evelyn Meyer'
            landlord_email = '+49 351 8382649'
            landlord_phone = 'vermietung@immobilien-galerie.de'

            description = ((((' '.join(response.css('.description .bodytext::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
            # # MetaData
            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value("external_source", self.external_source)  # String

            # item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", str(latitude)) # String
            item_loader.add_value("longitude", str(longitude)) # String
            item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

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
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR") # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
