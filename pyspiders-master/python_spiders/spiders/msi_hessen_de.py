# -*- coding: utf-8 -*-
# Author: Adham Mansour
from datetime import datetime
from math import ceil

import dateparser
import scrapy
from scrapy import Request, Selector

from ..helper import remove_unicode_char, extract_number_only, remove_white_spaces, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class Msi_hessenDeSpider(scrapy.Spider):
    name = 'msi_hessen_de'
    allowed_domains = ['msi-hessen.de']
    start_urls = ['https://www.msi-hessen.de/kaufen/immobilienangebote/?mt=rent&category=26&city&address&sort=sort%7Cdesc#immobilien']  # https not http
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
        rentals = response.css('.btn-block::attr(href)').extract()
        for rental in rentals:
            yield Request(url=rental,
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        external_id =((remove_unicode_char(remove_white_spaces(response.css('.lh-large::text').extract_first()))).split(': '))[-1]
        title = response.css('h1::text').extract_first()
        javascripts = response.css("script.vue-tabs::text").extract()
        desc_html_data = Selector(text=javascripts[0], type="html")
        description = ((((' '.join(desc_html_data.css('v-card-text p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        amenities_html_data = Selector(text=javascripts[1], type="html")
        list_items = amenities_html_data.css('li')
        list_items_dict = {}
        for list_item in list_items:
            headers = list_item.css('.key::text').extract_first()
            values = list_item.css('.value::text').extract_first()
            if values is None:
                values = list_item.css('.value span::attr(class)').extract_first()
                if 'times' in values:
                    values = False
                elif 'check' in values:
                    values = True
            list_items_dict[headers] = values
        is_rented = False
        if 'Vermietet:' in list_items_dict.keys():
            is_rented = list_items_dict['Vermietet:']
        if not is_rented:
            address = response.css('.clearfix::text').extract_first()
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

            property_type = response.css('.badge-secondary::text').extract_first()
            if 'haus' in property_type.lower():
                property_type = 'house'
            else:
                property_type ='apartment'

            square_meters = None  # METERS #int(response.css('::text').extract_first())
            if 'Wohnfläche\xa0(ca.):' in ' '.join(list_items_dict.keys()):
                square_meters  = list_items_dict['Wohnfläche\xa0(ca.):']
                square_meters = int(ceil(float(extract_number_only(square_meters))))
            room_count = None  # int(response.css('::text').extract_first())
            if 'Zimmer:' in list_items_dict.keys():
                room_count  = list_items_dict['Zimmer:']
                room_count = int(ceil(float(extract_number_only(room_count))))
            bathroom_count = None  # int(response.css('::text').extract_first())
            if 'Anzahl Badezimmer:' in list_items_dict.keys():
                bathroom_count = list_items_dict['Anzahl Badezimmer:']
                bathroom_count = int(ceil(float(extract_number_only(bathroom_count))))
            available_date = None  # response.css('.availability .meta-data::text').extract_first()
            if 'verfügbar ab:' in list_items_dict.keys():
                available_date = list_items_dict['verfügbar ab:']
            available_date = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if available_date:
                if available_date > datetime.now():
                    available_date = available_date.strftime("%Y-%m-%d")
                else:
                    available_date = None
            images = response.css('#exGallery a::attr(href)').extract()
            rent = None  # int(response.css('::text').extract_first())
            if 'Kaltmiete:' in list_items_dict.keys():
                rent = list_items_dict['Kaltmiete:']
                rent = int(ceil(float(extract_number_only(rent))))
            if 'Miete zzgl. NK:' in list_items_dict.keys():
                rent = list_items_dict['Miete zzgl. NK:']
                rent = int(ceil(float(extract_number_only(rent))))
            deposit = None
            if 'Kaution:' in list_items_dict.keys():
                deposit = list_items_dict['Kaution:']
                deposit = int(ceil(float(extract_number_only(deposit))))
            utilities = None
            if 'Nebenkosten:' in list_items_dict.keys():
                utilities = list_items_dict['Nebenkosten:']
                utilities = int(ceil(float(extract_number_only(utilities))))


            pets_allowed = None
            if 'Haustiere erlaubt:' in list_items_dict.keys():
                pets_allowed = list_items_dict['Haustiere erlaubt:']
            elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['pets_allowed']):
                pets_allowed = True

            furnished = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['furnished']):
                furnished = True

            parking = None
            if 'Garagenanzahl:' in list_items_dict.keys():
                parking = list_items_dict['Garagenanzahl:']
                parking = int(ceil(float(extract_number_only(parking))))
                if parking > 0:
                    parking = True
                elif parking == 0:
                    parking = False
                else:
                    parking = None
            elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['parking']):
                parking = True

            elevator = None
            if 'Fahrstuhl:' in list_items_dict.keys():
                elevator = list_items_dict['Fahrstuhl:']
                if elevator:
                    elevator = True
            elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['elevator']):
                elevator = True

            balcony = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['balcony']):
                balcony = True

            terrace = None
            if 'Anzahl Terrassen:' in list_items_dict.keys():
                terrace = list_items_dict['Anzahl Terrassen:']
                terrace = int(ceil(float(extract_number_only(terrace))))
                if terrace > 0:
                    terrace = True
                elif terrace == 0:
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

            landlord_name = response.css('strong::text').extract_first()
            landlord_name = (landlord_name.split(','))[0]
            if landlord_name is None:
                landlord_name = 'msi hessen'
            landlord_email = response.css('br+ a::text').extract_first()
            if landlord_email is None:
                landlord_email = 'info@msi-hessen.de'
            landlord_phone = response.css('.mb-4 p:nth-child(4)::text').extract_first()
            landlord_phone = (landlord_phone.split(':'))[1]
            landlord_phone = landlord_phone.replace(' ','')
            if landlord_phone is None:
                landlord_phone = '06631 776727'
            description = ((((' '.join(desc_html_data.css('v-card-text p:contains("Beschreibung") + p::text').extract()).replace('\n', '')).replace(
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

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
