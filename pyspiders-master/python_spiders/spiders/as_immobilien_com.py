# -*- coding: utf-8 -*-
# Author: Adham Mansour
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_coordinates,extract_location_from_address
from ..loaders import ListingLoader


class As_immobilienComSpider(scrapy.Spider):
    name = 'as_immobilien_com'
    allowed_domains = ['as-immobilien.com']
    start_urls = ['https://www.as-immobilien.com/index.php4?cmd=searchResults&goto=1&alias=suchmaske&kaufartids=2%2C3&kategorieids=100&ortnamegenau=']  # https not http
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
        rentals = response.css('.hauptinfos a::attr(href)').extract_first()
        yield Request(url='https://www.as-immobilien.com'+rentals,
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        list_items = response.css('.row.objektdaten .col-md-6')
        list_items_dict = {}
        for list_item in list_items:
            headers = ' '.join(list_item.css('.key ::text').extract())
            values = " ".join(list_item.css('.wert ::text').extract())
            list_items_dict[headers] = values
        external_id = None
        if 'Objekt-Nr.' in list_items_dict.keys():
            external_id = list_items_dict['Objekt-Nr.']
        title = response.css('h1::text').extract_first()
        description = ((((' '.join(response.css('.exposetexte p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        address = response.css('.fa.fa-map-marker::text').extract_first()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        property_type = 'apartment'
        square_meters = None  # METERS #int(response.css('::text').extract_first())
        if 'Wohnfläche  (ca.)' in list_items_dict.keys():
            square_meters = list_items_dict['Wohnfläche  (ca.)']
            square_meters = int(ceil(float(extract_number_only(square_meters))))
        room_count = None  # int(response.css('::text').extract_first())
        if 'Zimmer' in list_items_dict.keys():
            room_count = list_items_dict['Zimmer']
            room_count = int(ceil(float(extract_number_only(room_count))))
        bathroom_count = None  # int(response.css('::text').extract_first())
        if 'Badezimmer' in list_items_dict.keys():
            bathroom_count = list_items_dict['Badezimmer']
            bathroom_count = int(ceil(float(extract_number_only(bathroom_count))))

        available_date = None  # response.css('.availability .meta-data::text').extract_first()
        if 'bezugsfrei ab' in list_items_dict.keys():
            available_date = list_items_dict['bezugsfrei ab']
            if available_date.lower() != 'vermietet':
                if 'vereinbarung' in available_date.lower():
                    available_date = None
                elif 'sofort' in available_date.lower():
                    available_date = 'sofort'
                else:
                    available_date = available_date.split('.')
                    if len(available_date) == 3:
                        available_date = available_date[-1]+'-'+available_date[1]+'-'+available_date[0]
                    else:
                        available_date = available_date.split('-')
                        available_date = available_date[-1]+'-'+available_date[1]+'-'+available_date[0]

                images = response.css('img.focusview::attr(src)').extract()
                images = list(dict.fromkeys(images))
                floor_plan_images = None  # response.css('.one-photo-wrapper a::attr(href)').extract()
                rent = None  # int(response.css('::text').extract_first())
                if 'Kaltmiete' in list_items_dict.keys():
                    rent = list_items_dict['Kaltmiete']
                    rent = int(ceil(float(extract_number_only(rent))))

                deposit = None
                if 'Kaution' in list_items_dict.keys():
                    deposit = list_items_dict['Kaution']
                    deposit = int(ceil(float(extract_number_only(deposit))))

                prepaid_rent = None
                utilities = None
                if 'Nebenkosten (ca.)' in list_items_dict.keys():
                    utilities = list_items_dict['Nebenkosten (ca.)']
                    utilities = int(ceil(float(extract_number_only(utilities))))
                water_cost = None
                heating_cost = None
                energy_label = None
                if 'Energieausweis Werteklasse' in list_items_dict.keys():
                    external_id = list_items_dict['Energieausweis Werteklasse']

                floor = None  # response.css('::text').extract_first()
                if 'Etage' in list_items_dict.keys():
                    external_id = list_items_dict['Etage']
                pets_allowed = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['pets_allowed']):
                    pets_allowed = True

                furnished = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['furnished']):
                    furnished = True

                parking = None
                if 'Anzahl Garage / Stellplatz' in list_items_dict.keys():
                    parking = list_items_dict['Anzahl Garage / Stellplatz']
                    if int(parking) > 0:
                        parking = True
                    else:
                        parking = False

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
                description = ((((' '.join(response.css('.beschreibung p::text').extract()).replace('\n', '')).replace('\t', '')).replace('\r', '')))

                landlord_name = response.css('.kontaktname::text').extract_first()
                landlord_email = response.css('.col-lg-12 a::text').extract_first()
                landlord_phone = response.css('div.col-pt-12.col-qf-12.col-tb-12.col-md-12.col-sm-12.col-lg-12::text').extract()
                for item in landlord_phone:
                    if 'telefon' in item.lower():
                        landlord_phone = (item.split(': '))[1]

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
                current_last_page = response.css('.tar .visible-lg strong::text').extract_first()
                current_last_page = current_last_page.split(' | ')
                if current_last_page[0] != current_last_page [1]:
                    yield Request(url='https://www.as-immobilien.com' + response.css('.rightnav::attr(href)').extract_first(),
                                  callback=self.populate_item)
