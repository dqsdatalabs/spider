# -*- coding: utf-8 -*-
# Author: Adham Mansour
import re
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class Immobilien_canDeSpider(scrapy.Spider):
    name = 'immobilien_can_de'
    allowed_domains = ['immobilien-can.de']
    start_urls = ['https://www.immobilien-can.de/mietangebote.xhtml?f[35849-6]=miete&f[35849-18]=0&f[35849-16]=0']  # https not http
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
        rentals = response.css('.details-link::attr(href)').extract()
        for rental in rentals:
            yield Request(url=response.urljoin(rental),
                          callback=self.populate_item)
        pagination_list = response.css('.jumpbox-properties a')
        for button in pagination_list:
            if button.css('::text').extract_first() == '»':
                external_link = button.css('::attr(href)').extract_first()
                yield Request(url=response.urljoin(external_link),
                              callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        list_items = response.css('tr')
        list_items_dict = {}
        for list_item in list_items:
            headers = list_item.css('td strong::text').extract()
            values = list_item.css('td span::text').extract()
            if len(headers) == len(values):
                for n, header in enumerate(headers):
                    list_items_dict[header] = values[n]
        if list_items_dict['Objektart'] =='Wohnung':
            external_id = None # response.css('::text').extract_first()
            if 'ImmoNr' in list_items_dict.keys():
                external_id  = list_items_dict['ImmoNr']
            title = response.css('.headline h1::text').extract_first()
            description = ((((' '.join(response.css('#ausstattung::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
            description = (description.split('Ansprechpartne'))[0]
            lat_lng = response.css('script:contains("estate = ")').get()
            lat_lng = re.findall("\"(?:lat|lng)\": '(-?\d+.\d+)",lat_lng)
            latitude = lat_lng[0]
            longitude = lat_lng[1]
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            city = None
            if 'Ort' in list_items_dict.keys():
                city = list_items_dict['Ort']
            zipcode = None
            if 'PLZ' in list_items_dict.keys():
                zipcode  = list_items_dict['PLZ']

            property_type = 'apartment'  # response.css('::text').extract_first()
            square_meters = None  # METERS #int(response.css('::text').extract_first())
            if 'Wohnfläche' in list_items_dict.keys():
                square_meters = list_items_dict['Wohnfläche']
                square_meters = int(ceil(float(extract_number_only(square_meters))))
            room_count = None  # int(response.css('::text').extract_first())
            if 'Anzahl Zimmer' in list_items_dict.keys():
                room_count  = list_items_dict['Anzahl Zimmer']
                room_count = int(ceil(float(extract_number_only(room_count))))

            available_date = None  # response.css('.availability .meta-data::text').extract_first()

            images = response.css('li img::attr(src)').extract()
            images = [(i.split('@'))[0]for i in images]
            floor_plan_images = None  # response.css('.one-photo-wrapper a::attr(href)').extract()
            rent = None  # int(response.css('::text').extract_first())
            if 'Kaltmiete' in list_items_dict.keys():
                rent  = list_items_dict['Kaltmiete']
                rent = int(ceil(float(extract_number_only(rent))))
            deposit = None
            if 'Kaution' in list_items_dict.keys():
                deposit = list_items_dict['Kaution']
                deposit = int(ceil(float(extract_number_only(deposit))))
            prepaid_rent = None
            utilities = None
            if 'Nebenkosten' in list_items_dict.keys():
                utilities = list_items_dict['Nebenkosten']
                utilities = int(ceil(float(extract_number_only(utilities))))
            water_cost = None
            heating_cost = None
            if 'Heizkosten' in list_items_dict.keys():
                heating_cost = list_items_dict['Heizkosten']
                heating_cost = int(ceil(float(extract_number_only(heating_cost))))
            energy_label = ((((' '.join(response.css('#beschreibung p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
            energy_label = energy_label[-1]
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
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['parking']):
                parking = True

            elevator = None
            if 'Fahrstuhl' in list_items_dict.keys():
                elevator = list_items_dict['Fahrstuhl']
                if elevator.lower() == 'Fahrstuhl':
                    elevator = True
                elif elevator.lower() == 'kein fahrstuhl':
                    elevator = False
                else:
                    elevator = None
            elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['elevator']):
                elevator = True

            balcony = None
            if 'Balkon' in list_items_dict.keys():
                balcony = list_items_dict['Balkon']
                if balcony.lower() == 'ja':
                    balcony = True
                elif balcony.lower() == 'nein':
                    balcony = False
                else:
                    balcony = None
            elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['balcony']):
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

            landlord_name = 'Abdullah Can'
            landlord_email = 'vertrieb@immobilien-can.de'
            landlord_phone = '015208260276'

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
            # item_loader.add_value("bathroom_count", bathroom_count) # Int

            # item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

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
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
