# -*- coding: utf-8 -*-
# Author: Adham Mansour
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, remove_white_spaces, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class Gwg_gruppeDeSpider(scrapy.Spider):
    name = 'gwg_gruppe_de'
    allowed_domains = ['gwg-gruppe.de']
    start_urls = ['https://gwg-gruppe.de/zuhause-mieten?tx_openimmo_immobilie%5Baction%5D=search&tx_openimmo_immobilie%5Bcontroller%5D=Immobilie&cHash=1d9dbfcf098df7e0c3c5e6a4393f36b8#realestate-cards']  # https not http
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
        rentals = response.css('.card a::attr(href)').extract()
        for rental in rentals:
            yield Request(url='https://gwg-gruppe.de'+rental,
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css('.h2--blue .grid-cell.grid-cell-16 h2::text').extract_first()
        info_items = response.css('.h2--blue .grid-cell.grid-cell-6 p ::text').extract()
        info_items = [remove_white_spaces(((i.replace('\n','')).replace('\t',''))) for i in info_items]
        info_items = [i for i in info_items if ':' in i]
        info_items_dict = {}
        for info_item in info_items:
            splitted = info_item.split(':')
            header = splitted[0]
            value = splitted[1]
            info_items_dict[header] = value

        rent = None  # int(response.css('::text').extract_first())
        if 'Nettokaltmiete' in info_items_dict.keys():
            rent = info_items_dict['Nettokaltmiete']
            rent = int(ceil(float(extract_number_only(rent))))

        if rent is not None:
            external_id = None # response.css('::text').extract_first()
            if 'Objektnummer' in info_items_dict.keys():
                external_id = info_items_dict['Objektnummer']
            description = ((((' '.join(response.css('div.grid-cell.grid-cell-8.grid-post-2 p ::text,.tab div.grid-cell.grid-cell-6 p ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
            address = remove_white_spaces(((' '.join(response.css('.h2--blue .grid-cell.grid-cell-6 p:nth-child(1) ::text').extract()).replace('\n','')).replace('\t','')))
            latitude = None
            longitude = None
            city = None
            zipcode = None
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

            property_type = 'apartment'

            square_meters = None
            if 'Wohnfläche' in info_items_dict.keys():
                square_meters = info_items_dict['Wohnfläche']
                square_meters = int(ceil(float(extract_number_only(square_meters))))

            room_count = None  # int(response.css('::text').extract_first())
            if 'Anzahl Zimmer' in info_items_dict.keys():
                room_count = info_items_dict['Anzahl Zimmer']
                room_count = int(ceil(float(extract_number_only(room_count))))

            bathroom_count = None  # int(response.css('::text').extract_first())

            available_date = None  # response.css('.availability .meta-data::text').extract_first()
            if 'Verfügbar ab' in info_items_dict.keys():
                available_date = info_items_dict['Verfügbar ab']
                available_date = remove_white_spaces(available_date).split('.')
                if len(available_date) == 3:
                    available_date = available_date[-1]+'-'+available_date[1]+'-'+available_date[0]
                else:
                    available_date = 'available now'

            images = response.css('.slide__image.ar.ar--16-9 img::attr(src)').extract()
            images = ['https://gwg-gruppe.de'+i for i in images]
            floor_plan_images = response.css('div.slider.slider--immo.slider--full.slider--hidden img::attr(data-srcset)').extract_first()
            if floor_plan_images:
                floor_plan_images = floor_plan_images.split(',')
                floor_plan_images = ['https://gwg-gruppe.de'+i.replace(' 900w','').replace('\n','').replace('\t','') for i in floor_plan_images if '900w' in i]


            deposit = None
            if 'Kaution' in info_items_dict.keys():
                deposit = info_items_dict['Kaution']
                deposit = int(ceil(float(extract_number_only(deposit))))

            utilities = None
            if 'Nebenkosten' in info_items_dict.keys():
                utilities = info_items_dict['Nebenkosten']
                utilities = int(ceil(float(extract_number_only(utilities))))

            water_cost = None
            heating_cost = None
            if 'Heizkosten' in info_items_dict.keys():
                heating_cost = info_items_dict['Heizkosten']
                heating_cost = int(ceil(float(extract_number_only(heating_cost))))
            energy_label = None

            floor = None  # response.css('::text').extract_first()
            if 'Etage' in info_items_dict.keys():
                floor = info_items_dict['Etage']
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

            landlord_info_items = response.css('.grid-cell-16+ .grid-cell-8 p+ p ::text').extract()
            landlord_info_items = [remove_white_spaces(((i.replace('\n','')).replace('\t',''))) for i in landlord_info_items]
            landlord_info_items = [i for i in landlord_info_items if ':' in i]
            landlord_info_items_dict = {}
            for landlord_info_item in landlord_info_items:
                splitted = landlord_info_item.split(':')
                header = splitted[0]
                value = splitted[1]
                landlord_info_items_dict[header] = value

            if 'Ansprechpartner' in landlord_info_items_dict.keys():
                landlord_name = landlord_info_items_dict['Ansprechpartner']
            else:
                landlord_name = 'gwg gruppe'

            if 'E-Mail direkt' in landlord_info_items_dict.keys():
                landlord_email = response.css('.grid-cell-16+ .grid-cell-8 p+ p a::text').extract_first()
                landlord_email = landlord_email.replace('(at)','@')
                landlord_email = landlord_email.replace('(dot)','.')

            else:
                landlord_email = 'info@gwg-gruppe.de'

            if 'Telefon (Durchwahl)' in landlord_info_items_dict.keys():
                landlord_phone = landlord_info_items_dict['Telefon (Durchwahl)']

            else:
                landlord_phone = '0711 22777-0'
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
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array


            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR") # String

            # item_loader.add_value("water_cost", water_cost) # Int
            item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
