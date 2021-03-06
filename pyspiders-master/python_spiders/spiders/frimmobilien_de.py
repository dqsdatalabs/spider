# -*- coding: utf-8 -*-
# Author: Adham Mansour
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, remove_white_spaces, extract_location_from_address
from ..loaders import ListingLoader


class FrimmobilienDeSpider(scrapy.Spider):
    name = 'frimmobilien_de'
    # allowed_domains = ['frimmobilien.de']
    start_urls = ['https://www.frimmobilien.de/wohnung-miete/',
                  'https://www.frimmobilien.de/haus-miete/',
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
        rentals = response.css('.FFestateview-default-overview-estate a::attr(href)').extract()
        for rental in rentals:
            yield Request(url=rental,
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        list_items = response.css('#ff-default li')
        list_items_dict = {}
        for list_item in list_items:
            head_val = list_item.css('span::text').extract()
            head_val = [ (remove_white_spaces(i.replace('\n','').replace('\t',''))) for i in head_val]
            head_val = [i for i in head_val if i != '']
            headers = (head_val[0])
            values = (head_val[1])
            list_items_dict[headers] = values
        external_id = response.css('::text').extract_first()
        if 'Immobilien-ID' in list_items_dict.keys():
            external_id = list_items_dict['Immobilien-ID']
        title = response.css('.entry-title::text').extract_first()
        description = ((((' '.join(response.css('#ff-default p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        address = None  # response.css('::text').extract_first()
        if 'Lage' in list_items_dict.keys():
            address = list_items_dict['Lage']
        city = None
        zipcode = None
        if address:
            city = (address.split(' '))[1]
            zipcode = (address.split(' '))[0]
        longitude, latitude = extract_location_from_address(address)
        # zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        property_type = None  # response.css('::text').extract_first()
        if 'Art' in list_items_dict.keys():
            property_type = list_items_dict['Art']
            if 'wohnung' in property_type.lower():
                property_type = 'apartment'
            elif 'haus' in property_type.lower():
                property_type = 'house'
            else:
                property_type = None
        if property_type:
            square_meters = None  # METERS #int(response.css('::text').extract_first())
            if 'Wohnfl??che' in list_items_dict.keys():
                square_meters = list_items_dict['Wohnfl??che']
                square_meters = int(ceil(float(extract_number_only(square_meters))))
            room_count = None  # int(response.css('::text').extract_first())
            if 'Zimmer' in list_items_dict.keys():
                room_count = list_items_dict['Zimmer']
                room_count = int(ceil(float(extract_number_only(room_count))))
            bathroom_count = None  # int(response.css('::text').extract_first())
            if 'badezimmer' in description.lower():
                bathroom_count = 1

            images = response.css('.FFestateview-default-details-main .FFestateview-default-details-main-image img::attr(src)').extract()
            floor_plan_images = response.css('.FFestateview-default-groundplot img::attr(src)').extract()
            rent = None  # response.css('::text').extract_first()
            if 'Warmmiete' in list_items_dict.keys():
                rent = list_items_dict['Warmmiete']
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
            energy_label = None
            if 'Energieeffizienzklasse' in list_items_dict.keys():
                energy_label = list_items_dict['Energieeffizienzklasse']
            floor = None  # response.css('::text').extract_first()

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

            landlord_name = 'Florian Ruppert'
            landlord_email = 'info@frimmobilien.de'
            landlord_phone = '+49 6048 9525773'
            description = ((((' '.join(response.css('#ff-default > div:nth-child(3) > div > p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
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
