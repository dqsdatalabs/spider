# -*- coding: utf-8 -*-
# Author: Adham Mansour
import re
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_coordinates, \
    extract_location_from_address


class ImmobilienvogelDeSpider(scrapy.Spider):
    name = 'immobilienvogel_de'
    allowed_domains = ['immobilienvogel.de']
    start_urls = ['https://immobilienvogel.de/unsere-objekte/mietobjekte-2/']  # https not http
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    keywords = {
        'pets_allowed': ['Haustiere erlaubt'],
        'furnished': ['m bliert', 'ausstattung'],
        'parking': ['tiefgarage','garage', 'Stellplatz' 'Parkh user','Parkplatz'],
        'elevator': ['fahrstuhl', 'aufzug'],
        'balcony': ['balkon'],
        'terrace': ['terrasse'],
        'swimming_pool': ['baden', 'schwimmen', 'schwimmbad', 'pool', 'Freibad'],
        'washing_machine': ['waschen', 'w scherei', 'waschmaschine','waschk che'],
        'dishwasher': ['geschirrspulmaschine', 'geschirrsp ler',],
        'floor' : ['etage'],
        'bedroom': ['Schlafzimmer']
    }
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('.oo-listtitle a::attr(href)').extract()
        for rental in rentals:
            yield Request(url=rental,
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        external_id = re.findall('objektdetails\/(\d+)',response.url)
        external_id[0]
        title = response.css('h1::text').extract_first()
        if not 'Garagenstellplätze' in title:
            description = remove_unicode_char((((' '.join(response.css('.pt-0::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))

            property_type = 'apartment'
            list_items = response.css('.oo-detailslisttd::text').extract()
            keys = []
            values = []
            for n,list_item in enumerate(list_items,1):
                if (n % 2) == 0:
                    values.append((list_item))
                else:
                    keys.append((list_item))
            list_items_dict = zip(keys, values)
            list_items_dict = dict(list_items_dict)

            address = None  # response.css('::text').extract_first()
            if 'Ort' in list_items_dict.keys():
                address = list_items_dict['Ort']
            zipcode = None
            if 'PLZ' in list_items_dict.keys():
                zipcode = list_items_dict['PLZ']
            longitude, latitude = extract_location_from_address(address+', '+zipcode)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

            square_meters = None  # METERS #int(response.css('::text').extract_first())
            if 'Wohnfläche' in list_items_dict.keys():
                square_meters = list_items_dict['Wohnfläche']
                square_meters = int(ceil(float(extract_number_only(square_meters))))

            room_count = None  # int(response.css('::text').extract_first())
            if 'Anzahl Zimmer' in list_items_dict.keys():
                room_count = list_items_dict['Anzahl Zimmer']
                room_count = int(ceil(float(extract_number_only(room_count))))

            bathroom_count = None  # int(response.css('::text').extract_first())
            if 'Anzahl Badezimmer' in list_items_dict.keys():
                bathroom_count = list_items_dict['Anzahl Badezimmer']
                bathroom_count = int(ceil(float(extract_number_only(bathroom_count))))

            images = response.css('.oo-detailspicture img::attr(src)').extract()

            rent = None  # int(response.css('::text').extract_first())
            if 'Kaltmiete' in list_items_dict.keys():
                rent = list_items_dict['Kaltmiete']
                rent = int(ceil(float(extract_number_only(rent))))


            deposit = None
            if 'Kaution' in list_items_dict.keys():
                deposit = list_items_dict['Kaution']
                if len(str(extract_number_only(deposit))) == 1:
                    deposit = rent * int(extract_number_only(deposit))
                else:
                    deposit = int(ceil(float(extract_number_only(deposit))))

            prepaid_rent = None
            utilities = None
            if 'Nebenkosten' in list_items_dict.keys():
                utilities = list_items_dict['Nebenkosten']
                utilities = int(ceil(float(extract_number_only(utilities))))

            water_cost = None
            heating_cost = None
            energy_label = None
            if 'Energieeffizienzklasse' in list_items_dict.keys():
                energy_label = list_items_dict['Energieeffizienzklasse']

            floor = None  # response.css('::text').extract_first()
            if 'Etage' in list_items_dict.keys():
                floor = list_items_dict['Etage']


            furnished = None
            if any(word in description.lower() for word in self.keywords['furnished']):
                furnished = True

            parking = None
            if any(word in description.lower() for word in self.keywords['parking']):
                parking = True

            elevator = None
            if any(word in description.lower() for word in self.keywords['elevator']):
                elevator = True

            balcony = None
            if any(word in description.lower() for word in self.keywords['balcony']):
                balcony = True
            else:
                if 'Balkon' in list_items_dict.keys():
                    balcony = list_items_dict['Balkon']
                    if balcony.lower == 'ja':
                        balcony = True
                    elif balcony.lower == 'nein':
                        balcony = False
                    else:
                        balcony = None

            terrace = None
            if any(word in description.lower() for word in self.keywords['terrace']):
                terrace = True

            swimming_pool = None
            if any(word in description.lower() for word in self.keywords['swimming_pool']):
                swimming_pool = True

            washing_machine = None
            if any(word in description.lower() for word in self.keywords['washing_machine']):
                washing_machine = True

            dishwasher = None
            if any(word in description.lower() for word in self.keywords['dishwasher']):
                dishwasher = True

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
            item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            # item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

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

            item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", 'Vogel Immobilien') # String
            item_loader.add_value("landlord_phone", '0621 - 18 15 666') # String

            item_loader.add_value("landlord_email", 'info@immobilienvogel.de') # String

            self.position += 1
            yield item_loader.load_item()
