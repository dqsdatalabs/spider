# -*- coding: utf-8 -*-
# Author: Adham Mansour
import re
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates, remove_white_spaces
from ..loaders import ListingLoader


class Kuenzl_immobilienDeSpider(scrapy.Spider):
    name = 'kuenzl_immobilien_de'
    allowed_domains = ['kuenzl-immobilien.de']
    start_urls = ['https://www.kuenzl-immobilien.de/aktuelle-objekte-vermieten/']  # https not http
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
        rentals = response.css('section.l-section')
        for rental in rentals:
            if rental.css('.l-section .w-separator') == []:
                item_loader = ListingLoader(response=response)
                title = rental.css('#expose-title::text').extract_first()
                if title:
                    if not 'vermietet' in title.lower():
                        description = ((((' '.join(rental.css('.is24-long-text-attribute ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
                        amenities = rental.css('.font-s strong::text').extract()
                        rent = None
                        square_meters = None
                        room_count = None
                        utilities = None
                        for i in amenities:
                            if i != '\n':
                                i = (i.split('/'))
                                if len(i) == 1:
                                    i = (i[0].split('–'))
                                for j in i:
                                    if 'm²' in j:
                                        square_meters = int(ceil(float(extract_number_only(j))))
                                    elif 'Räume' in j or 'Zimmer' in j:
                                        room_count = extract_number_only(j)
                                    elif 'Nebenkosten' in j:
                                        utilities = int(ceil(float(extract_number_only(j))))
                                    elif 'Kaltmiete' in j:
                                        rent = int(ceil(float(extract_number_only(remove_unicode_char(j)))))
                        address = re.findall('((\w+):|(?:von|am) \w+)',title)
                        address = address[0][0]
                        address = address +', Germany'
                        longitude, latitude = extract_location_from_address(address)
                        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

                        bathroom_count = re.findall('(\d) b\u00e4der',description.lower())  # int(response.css('::text').extract_first())
                        if bathroom_count == []:
                            bathroom_count = 1

                        images = rental.css('.rsImg::attr(href)').extract()

                        floor = None
                        if 'erdgeschoss' in description.lower():
                            floor = 'Erdgeschoss'
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

                        landlord_name = 'Künzl Immobilien'
                        landlord_email = 'hk@kuenzl-immobilien.de'
                        landlord_phone = '+49 8106 995256'
                        description = (description.split('Ausstattung'))[0]
                        description = ((remove_white_spaces(description)).replace('Objektbeschreibung',''))
                        # # MetaData
                        item_loader.add_value("external_link", response.url+'#'+str(self.position))  # String
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
                        item_loader.add_value("property_type", 'apartment') # String
                        item_loader.add_value("square_meters", square_meters) # Int
                        item_loader.add_value("room_count", room_count) # Int
                        item_loader.add_value("bathroom_count", bathroom_count) # Int

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
                        # item_loader.add_value("deposit", deposit) # Int
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
