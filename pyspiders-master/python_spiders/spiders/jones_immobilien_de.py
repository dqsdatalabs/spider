# -*- coding: utf-8 -*-
# Author: Adham Mansour
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only
from ..loaders import ListingLoader
import dateparser
from datetime import datetime

class Jones_immobilienDeSpider(scrapy.Spider):
    name = 'jones_immobilien_de'
    allowed_domains = ['jones-immobilien.de']
    start_urls = ['https://jones-immobilien.de/angebote/miete/haus/',
                  'https://jones-immobilien.de/angebote/miete/mietwohnung'
                  ]  # https not http
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    keywords = {
        'pets_allowed': ['Haustiere erlaubt'],
        'furnished': ['m bliert', 'ausstattung'],
        'parking': ['garage', 'Stellplatz' 'Parkh user','Parkplatz'],
        'elevator': ['fahrstuhl', 'aufzug'],
        'balcony': ['balkon'],
        'terrace': ['terrasse'],
        'swimming_pool': [' baden', 'schwimmen', 'schwimmbad', 'pool', 'Freibad'],
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
        rentals = response.css('.immobox')
        for rental in rentals:
            yield Request(url='https://jones-immobilien.de/'+rental.css('a::attr(href)').extract_first(),
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        list_items = response.css('table.table tr')
        list_items_dict = {}
        for list_item in list_items:
            headers = ' '.join(list_item.css('td:nth-child(1)::text').extract())
            values = ((((' '.join(list_item.css('td:nth-child(2) ::text').extract())).replace('\n','')).replace('\t','')).replace(' ',''))
            list_items_dict[headers] = values
        external_id = None # response.css('::text').extract_first()
        title =  response.css('h2::text').extract_first()
        if not 'vermietet' in title.lower():
            description = ((((' '.join(response.css('p:nth-child(2) ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
            address = None  # response.css('::text').extract_first()
            latitude = None
            longitude = None
            zipcode = None
            # longitude, latitude = extract_location_from_address(address)
            # zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            city = 'Idstein'
            property_type = 'apartment'  # response.css('::text').extract_first()
            if 'haus' in title.lower():
                property_type = 'house'
            square_meters = None
            if 'Wohnfläche' in list_items_dict.keys():
                square_meters = list_items_dict['Wohnfläche']
                square_meters = int(ceil(float(extract_number_only(square_meters))))
            room_count = None  # int(response.css('::text').extract_first())
            if 'Zimmer' in list_items_dict.keys():
                room_count = list_items_dict['Zimmer']
                room_count = int(ceil(float(extract_number_only(room_count))))
            if room_count is None:
                room_count = 1

            bathroom_count = None  # int(response.css('::text').extract_first())
            if 'Bäder' in list_items_dict.keys():
                bathroom_count = list_items_dict['Bäder']
                bathroom_count = int(ceil(float(extract_number_only(bathroom_count))))
            if bathroom_count is None:
                bathroom_count = 1
            available_date = None  # response.css('.availability .meta-data::text').extract_first()
            if 'Bezugsfrei ab' in list_items_dict.keys():
                available_date = list_items_dict['Bezugsfrei ab']
                available_date = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if available_date > datetime.now():
                    available_date = available_date.strftime("%Y-%m-%d")
                else:
                    available_date = None
            images = response.css('.thumbnails.span9 noscript img::attr(src)').extract()
            images = ['https://jones-immobilien.de/' + (i.split('?'))[0] for i in images]
            rent = None  # int(response.css('::text').extract_first())
            if 'Kaltmiete' in list_items_dict.keys():
               rent = list_items_dict['Kaltmiete']
               rent = int(ceil(float(extract_number_only(remove_unicode_char(rent)))))

            elif 'Mietpreis' in list_items_dict.keys():
               rent = list_items_dict['Mietpreis']
               rent = int(ceil(float(extract_number_only((rent)))))
            if rent:
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
                    energy_label  = list_items_dict['Energieeffizienzklasse']

                pets_allowed = None
                if 'Haustiere' in list_items_dict.keys():
                    pets_allowed = list_items_dict['Haustiere']
                    if pets_allowed.lower() == 'ja' or pets_allowed.lower() == 'vorhanden':
                        pets_allowed = True
                    elif pets_allowed.lower() == 'nein' or pets_allowed.lower() == 'nichtvorhanden':
                        pets_allowed = False
                    else:
                        pets_allowed = None
                elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['pets_allowed']):
                    pets_allowed = True
                #
                furnished = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['furnished']):
                    furnished = True

                parking = None
                if 'Stellplätz A' in list_items_dict.keys():
                    parking = list_items_dict['Stellplätz A']
                    if parking != '':
                        parking = True
                    elif parking.lower() == 'nein' or parking.lower() == 'nichtvorhanden':
                        parking = False
                    else:
                        parking = None
                elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['parking']):
                    parking = True

                elevator = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['elevator']):
                    elevator = True

                balcony = None
                if 'Balkon' in list_items_dict.keys():
                    balcony = list_items_dict['Balkon']
                    if balcony.lower() == 'ja' or balcony.lower() == 'vorhanden':
                        balcony = True
                    elif balcony.lower() == 'nein' or balcony.lower() == 'nichtvorhanden':
                        balcony = False
                    else:
                        balcony = None
                elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['balcony']):
                    balcony = True
                #
                terrace = None
                if 'Terasse' in list_items_dict.keys():
                    terrace = list_items_dict['Terasse']
                    if terrace.lower() == 'ja' or terrace.lower() == 'vorhanden':
                        terrace = True
                    elif terrace.lower() == 'nein' or terrace.lower() == 'nichtvorhanden':
                        terrace = False
                    else:
                        terrace = None
                elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['terrace']):
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

                landlord_name = response.css('#sidebar .fadeIn strong::text').extract_first()
                landlord_email = response.css('tr:nth-child(1) a::text').extract_first()
                landlord_phone = response.css('tr+ tr a::text').extract_first()
                description = ((((' '.join(response.css('p+ p ::text').extract()).replace('\n', '')).replace(
                    '\t', '')).replace('\r', '')))

                # # MetaData
                item_loader.add_value("external_link", response.url)  # String
                item_loader.add_value("external_source", self.external_source)  # String

                # item_loader.add_value("external_id", external_id) # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", title) # String
                item_loader.add_value("description", description) # String

                # # Property Details
                item_loader.add_value("city", city) # String
                # item_loader.add_value("zipcode", zipcode) # String
                # item_loader.add_value("address", address) # String
                # item_loader.add_value("latitude", latitude) # String
                # item_loader.add_value("longitude", longitude) # String
                # item_loader.add_value("floor", floor) # String
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
                item_loader.add_value("landlord_name", landlord_name) # String
                item_loader.add_value("landlord_phone", landlord_phone) # String
                item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()
