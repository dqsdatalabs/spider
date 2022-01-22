# -*- coding: utf-8 -*-
# Author: Adham Mansour
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class ArnewulfDeSpider(scrapy.Spider):
    name = 'arnewulf_de'
    allowed_domains = ['arnewulf.de']
    start_urls = ['https://www.arnewulf.de/mietobjekte.xhtml?f[622-17]=miete']  # https not http
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
        rentals = response.css('.object-frame .fourth')
        for rental in rentals:
            external_link = rental.css('.image a::attr(href)').extract_first()
            if rental.css('.status-reserved') == [] and rental.css('.status-rented') == []:
                yield Request(url='https://www.arnewulf.de/'+external_link,
                              callback=self.populate_item)
        pagination_list = response.css('.jumpbox-frame a')
        for button in pagination_list:
            button_text = button.css("::text").extract_first()
            if button_text == '»':
                yield Request(url='https://www.arnewulf.de/'+button.css('::attr(href)').extract_first(),
                              callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        list_items = response.css('.details-desktop tr')
        list_items_dict = {}
        for list_item in list_items:
            headers = list_item.css('td strong::text').extract()
            values = list_item.css('td span::text').extract()
            if len(headers) == len(values):
                for n, header in enumerate(headers):
                    list_items_dict[header] = values[n]
        if 'Nutzungsart' in list_items_dict.keys():
            property_type = list_items_dict['Nutzungsart']
        if property_type == 'Wohnen':
            external_id = None  # response.css('::text').extract_first()
            if 'externe Objnr' in list_items_dict.keys():
                external_id = list_items_dict['externe Objnr']
            title = (response.css('h2::text').extract())[1]
            description = ((((' '.join(response.css('.information span ::text').extract()).replace('\n', '')).replace('\t','')).replace('\r', '')))

            city = None
            if 'Ort' in list_items_dict.keys():
                city = list_items_dict['Ort']
            zipcode = None
            if 'PLZ' in list_items_dict.keys():
                zipcode = list_items_dict['PLZ']
            address = 'Deutschland, ' + city + ', ' + zipcode
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            if address == zipcode:
                address = address + ', ' + city
            if 'Ort' in list_items_dict.keys():
                city = list_items_dict['Ort']
            zipcode = None
            if 'PLZ' in list_items_dict.keys():
                zipcode = list_items_dict['PLZ']
            property_type = None  # response.css('::text').extract_first()
            if 'Objektart' in list_items_dict.keys():
                property_type = list_items_dict['Objektart']
                if 'villa' in property_type.lower() or 'haus' in property_type.lower():
                    property_type = 'house'
                else:
                    property_type = 'apartment'

            square_meters = None  # METERS #int(response.css('::text').extract_first())
            if 'Wohnfläche' in list_items_dict.keys():
                square_meters = list_items_dict['Wohnfläche']
                square_meters = int(ceil(float(extract_number_only(remove_unicode_char(square_meters)))))

            room_count = None  # int(response.css('::text').extract_first())
            if 'Anzahl Zimmer' in list_items_dict.keys():
                room_count = list_items_dict['Anzahl Zimmer']
                room_count = int(ceil(float(extract_number_only(remove_unicode_char(room_count)))))

            available_date = None  # int(response.css('::text').extract_first())
            if 'Verfügbar ab (Text)' in list_items_dict.keys():
                available_date = list_items_dict['Verfügbar ab (Text)']

            bathroom_count = None  # response.css('.availability .meta-data::text').extract_first()
            if 'Anzahl Badezimmer' in list_items_dict.keys():
                bathroom_count = list_items_dict['Anzahl Badezimmer']
                bathroom_count = int(ceil(float(extract_number_only(remove_unicode_char(bathroom_count)))))
            elif 'badezimmer' in description.lower():
                bathroom_count = 1

            images = response.css('.gallery li a::attr(href)').extract()
            floor_plan_images = None  # response.css('.one-photo-wrapper a::attr(href)').extract()

            rent = None  # int(response.css('::text').extract_first())
            if 'Kaltmiete' in list_items_dict.keys():
                rent = list_items_dict['Kaltmiete']
                rent = int(ceil(float(extract_number_only(remove_unicode_char(rent)))))

            deposit = None
            if 'Kaution' in list_items_dict.keys():
                deposit = list_items_dict['Kaution']
                deposit = int(ceil(float(extract_number_only(remove_unicode_char(deposit)))))
            utilities = None
            if 'Nebenkosten' in list_items_dict.keys():
                utilities = list_items_dict['Nebenkosten']
                utilities = int(ceil(float(extract_number_only(remove_unicode_char(utilities)))))

            floor = None
            if 'Etage' in list_items_dict.keys():
                floor = list_items_dict['Etage']

            pets_allowed = None
            if 'Haustiere' in list_items_dict.keys():
                pets_allowed = list_items_dict['Haustiere']
                if pets_allowed.lower() == 'ja':
                    pets_allowed = True
                elif pets_allowed.lower() == 'nein':
                    pets_allowed = False
                else:
                    pets_allowed = None
            else:
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['pets_allowed']):
                    pets_allowed = True

            furnished = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['furnished']):
                furnished = True

            parking = None
            if 'Anzahl Stellplätze' in list_items_dict.keys():
                parking = list_items_dict['Anzahl Stellplätze']
                if int(parking) > 0:
                    parking = True
                else:
                    parking = False

            elevator = None
            if 'Fahrstuhl' in list_items_dict.keys():
                elevator = list_items_dict['Fahrstuhl']
                if elevator.lower() == 'kein fahrstuhl':
                    elevator = False
                else:
                    elevator = True
            else:
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['elevator']):
                    elevator = True

            balcony = None
            terrace = None
            if 'Balkon/Terrasse' in list_items_dict.keys():
                balcony = list_items_dict['Balkon/Terrasse']
                if balcony.lower() == 'ja':
                    balcony = True
                    terrace = True
                elif balcony.lower() == 'nein':
                    balcony = False
                    terrace = False
            else:
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

            landlord_name = response.css('.center > strong::text').extract_first()
            landlord_email = 'info@arnewulf.de'
            landlord_phone = response.css('.center span span::text').extract_first()
            description = ((((' '.join(response.css('.information span:nth-child(1) span::text').extract()).replace('\n', '')).replace(
                '\t', '')).replace('\r', '')))

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
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
