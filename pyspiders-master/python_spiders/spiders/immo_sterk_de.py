# -*- coding: utf-8 -*-
# Author: Adham Mansour
import re
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates, remove_white_spaces
from ..loaders import ListingLoader


class Immo_sterkDeSpider(scrapy.Spider):
    name = 'immo_sterk_de'
    allowed_domains = ['immo-sterk.de']
    start_urls = ['https://www.immo-sterk.de/vermieten-wohnen/']  # https not http
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    headers = []
    keywords = {
        'pets_allowed': ['Haustiere erlaubt'],
        'furnished': ['m bliert', 'ausstattung'],
        'parking': ['garage', 'Stellplatz' 'Parkh user'],
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
        rentals = response.css('.col-md-4')
        neglect = ['geschäft','gewer','laden','lager','ausstellungs','büro']
        for rental in rentals:
            if not rental.css('.property-status-reserviert'):
                title = rental.css('.property-title a::text').extract_first()
                if not any(word in title.lower() for word in neglect):
                    external_link = rental.css('.property-details a::attr(href)').extract_first()
                    yield Request(url=external_link,
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = None
        for i in response.css('meta'):
            header = i.css('::attr(property)').extract_first()
            if header =='og:title':
                title = i.css('::attr(content)').extract_first()
        external_id = response.css('h4::text').extract_first()
        external_id = (external_id.split(': '))[-1]
        description = remove_white_spaces((((' '.join(response.css('#pp-accordion-5c6a7c45a0e6e-panel-0 p ::text').extract()).replace('\n','')).replace('\t', '')).replace('\r', '')))
        amenities  = ((((' '.join(response.css('#pp-accordion-5c6a7c45a0e6e-panel-1 p ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        property_type = 'apartment'
        square_meters = None
        parking = None
        city = None
        rent = None
        room_count = None
        for info_boxes in response.css('.fl-col-group .fl-col-small'):
            header = info_boxes.css('.detailhead p::text').extract_first()
            self.headers.append(header)
            if header == 'Gesamtfläche' or header == 'Wohnfläche':
                square_meters = info_boxes.css('.detailtext .fl-heading-text::text').extract_first()
                if square_meters:
                    square_meters = int(extract_number_only(extract_number_only(square_meters)))
            elif header == 'Stellplätze':
                parking = info_boxes.css('.detailtext .fl-heading-text::text').extract_first()
                if parking.isnumeric():
                    parking = True
                else:
                    parking = False
            elif header == 'Lage':
                city = info_boxes.css('.detailtext .fl-heading-text::text').extract_first()
            elif header == 'Warmmiete' or header == 'Kaltmiete':
                rent = info_boxes.css('.detailtext .fl-heading-text::text').extract_first()
                if rent:
                    rent = int(extract_number_only(extract_number_only(rent)))
            elif header == 'Zimmer':
                room_count = info_boxes.css('.detailtext .fl-heading-text::text').extract_first()
                if room_count:
                    room_count = int(ceil(float((extract_number_only(room_count)))))
            elif header == 'Verfügbar ab':
                available_date = info_boxes.css('.detailtext .fl-heading-text::text').extract_first()
                if available_date =='sofort':
                    available_date = 'available now'
                elif available_date:
                    if '/' in available_date:
                        available_date = available_date.split('/')
                        available_date = available_date[-1] + '-' + available_date[1] + '-' + available_date[0]
                    elif '.' in available_date:
                        available_date = available_date.split('.')
                        available_date = available_date[-1] + '-' + available_date[1] + '-' + available_date[0]
                    elif '-' in available_date:
                        available_date = available_date.split('-')
                        available_date = available_date[0] + '-' + available_date[1] + '-' + available_date[-1]
        if rent:
            longitude = None
            latitude = None
            zipcode = None
            address = None
            if city:
                longitude, latitude = extract_location_from_address(city+', Germany')
                zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            item_loader = ListingLoader(response=response)
            bathroom_count = re.findall('\*  (\d)[\w\s]*[(wc)|(bäd)]? \*',amenities.lower())
            if len(bathroom_count) > 0:
                bathroom_count = bathroom_count[0]
            else:
                if ' wc '  in amenities or 'bäd' in amenities.lower():
                    bathroom_count = 1
                else:
                    bathroom_count = None


            images = response.css('.fl-mosaicflow-item a::attr(href)').extract()
            utilities = re.findall('nebenkosten[\w]*:?\s(\\u20ac )?(\d+)',description.lower())
            if utilities:
                utilities = int(utilities[0][-1])
            else:
                utilities = None
            energy_label = response.css('.epass-diagram .active ::text').extract_first()
            if energy_label:
                energy_label = energy_label[0]
            else:
                energy_label = None

            pets_allowed = None
            if any(word in description.lower() for word in self.keywords['pets_allowed']) or any(
                    word in amenities.lower() for word in self.keywords['pets_allowed']):
                pets_allowed = True

            furnished = None
            if any(word in description.lower() for word in self.keywords['furnished']) or any(
                    word in amenities.lower() for word in self.keywords['furnished']):
                furnished = True

            elevator = None
            if any(word in description.lower() for word in self.keywords['elevator']) or any(
                    word in amenities.lower() for word in self.keywords['elevator']):
                elevator = True

            balcony = None
            if any(word in description.lower() for word in self.keywords['balcony']) or any(
                    word in amenities.lower() for word in self.keywords['balcony']):
                balcony = True

            terrace = None
            if any(word in description.lower() for word in self.keywords['terrace']) or any(
                    word in amenities.lower() for word in self.keywords['terrace']):
                terrace = True

            swimming_pool = None
            if any(word in description.lower() for word in self.keywords['swimming_pool']) or any(
                    word in amenities.lower() for word in self.keywords['swimming_pool']):
                swimming_pool = True

            washing_machine = None
            if any(word in description.lower() for word in self.keywords['washing_machine']) or any(
                    word in amenities.lower() for word in self.keywords['washing_machine']):
                washing_machine = True

            dishwasher = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['dishwasher']) or any(
                    word in remove_unicode_char(amenities.lower()) for word in self.keywords['dishwasher']):
                dishwasher = True

            landlord_name = response.css('.fl-rich-text p:nth-child(2)::text').extract_first()
            landlord_phone = response.css('.fl-rich-text p:nth-child(4)::text').extract()
            landlord_email = landlord_phone[-1]
            landlord_phone = landlord_phone[0]

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
            # item_loader.add_value("deposit", ) # Int
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
