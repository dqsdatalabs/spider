# -*- coding: utf-8 -*-
# Author: Adham Mansour
from datetime import datetime
from math import ceil

import dateparser
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates, remove_white_spaces
from ..loaders import ListingLoader


class Joachim_roemer_immobilienDeSpider(scrapy.Spider):
    name = 'joachim_roemer_immobilien_de'
    allowed_domains = ['joachim-roemer-immobilien.de']
    start_urls = ['https://joachim-roemer-immobilien.de/for-rent/?limit=100&wplpage=1']  # https not http
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
        rentals = response.css('.wpl_prp_bot a::attr(href)').extract()
        for rental in rentals:
            yield Request(url=response.urljoin(rental),
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        list_items = response.css('.rows')
        list_items_dict = {}
        for list_item in list_items:
            headers = remove_white_spaces(list_item.css('::text').extract_first())
            values = (list_item.css('span::text').extract_first())
            if values:
                values = remove_white_spaces((values))
            else:
                values = 'empty'
            list_items_dict[headers] = values
        external_id = None # response.css('::text').extract_first()
        if 'Referenz ID :' in list_items_dict.keys():
            external_id = list_items_dict['Referenz ID :']
        title = response.css('.title_text::text').extract_first()
        description = ((((' '.join(response.css('.wpl_prp_show_detail_boxes_cont p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        address = response.css('.wpl-location::text').extract_first()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        property_type = None  # response.css('::text').extract_first()
        if 'Art der Immobilie :' in list_items_dict.keys():
            property_type = list_items_dict['Art der Immobilie :']
        if 'wohnung' in property_type.lower():
            property_type = 'apartment'
            square_meters = None  # METERS #int(response.css('::text').extract_first())
            if 'Wohnfläche :' in list_items_dict.keys():
                square_meters = list_items_dict['Wohnfläche :']
                square_meters = int(ceil(float(extract_number_only(square_meters.replace('.',',')))))
            room_count = None  # int(response.css('::text').extract_first())
            if 'Zimmer :' in list_items_dict.keys():
                room_count = list_items_dict['Zimmer :']
                room_count = int(ceil(float(extract_number_only(room_count))))
            bathroom_count = None  # int(response.css('::text').extract_first())
            if 'Badezimmer :' in list_items_dict.keys():
                bathroom_count = list_items_dict['Badezimmer :']
                bathroom_count = int(ceil(float(extract_number_only(bathroom_count))))

            available_date = None  # response.css('.availability .meta-data::text').extract_first()
            if 'Mietbeginn :' in list_items_dict.keys():
                available_date = list_items_dict['Mietbeginn :']
                if available_date.lower() == 'nach vereinbarung':
                    available_date = None
                else:
                    if (available_date[0]).isnumeric():
                        available_date = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                        if available_date > datetime.now():
                            available_date = available_date.strftime("%Y-%m-%d")
                        else:
                            available_date = None
            images = response.css('.wpl-gallery-pshow li::attr(data-src)').extract()
            rent = None  # int(response.css('::text').extract_first())
            if 'Nettokaltmiete :' in list_items_dict.keys():
                rent = list_items_dict['Nettokaltmiete :']
                rent = int(ceil(float(extract_number_only(rent))))
            if rent:
                deposit = None
                if 'Kaution :' in list_items_dict.keys():
                    deposit = list_items_dict['Kaution :']
                    deposit = int(ceil(float(extract_number_only(deposit))))
                utilities = None
                if 'Betriebskosten :' in list_items_dict.keys():
                    utilities = list_items_dict['Betriebskosten :']
                    utilities = int(ceil(float(extract_number_only(utilities))))
                heating_cost = None
                if 'Heizkosten :' in list_items_dict.keys():
                    heating_cost = list_items_dict['Heizkosten :']
                    heating_cost = int(ceil(float(extract_number_only(heating_cost))))
                energy_label = None
                if 'Energieeffizienzklasse :' in list_items_dict.keys():
                    energy_label = list_items_dict['Energieeffizienzklasse :']
                floor = None  # response.css('::text').extract_first()
                if 'Etage :' in list_items_dict.keys():
                    floor = list_items_dict['Etage :']
                pets_allowed = None
                if 'Haustiere :' in list_items_dict.keys():
                    pets_allowed = list_items_dict['Haustiere :']
                    if pets_allowed.lower() == 'erlaubt':
                        pets_allowed == True
                    else:
                        pets_allowed == False
                elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['pets_allowed']):
                    pets_allowed = True

                furnished = None
                if 'möbliert :' in list_items_dict.keys():
                    furnished = True
                elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['furnished']):
                    furnished = True

                parking = None
                if 'Parkplatz :' in list_items_dict.keys():
                    parking = True
                elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['parking']):
                    parking = True

                elevator = None
                if 'Aufzug' in list_items_dict.keys():
                    elevator = True
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['elevator']):
                    elevator = True

                balcony = None
                if 'Balkon' in list_items_dict.keys():
                    balcony = True
                elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['balcony']):
                    balcony = True

                terrace = None
                if 'Terrasse' in list_items_dict.keys() or 'Dachterasse' in list_items_dict.keys():
                    terrace = True
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
                item_loader.add_value("heating_cost", heating_cost) # Int

                item_loader.add_value("energy_label", energy_label) # String

                # # LandLord Details
                item_loader.add_value("landlord_name", 'Joachim Römer Immobilien') # String
                item_loader.add_value("landlord_phone", '040 2351 8129') # String
                item_loader.add_value("landlord_email", 'info@joachim-roemer-immobilien.de') # String

                self.position += 1
                yield item_loader.load_item()
