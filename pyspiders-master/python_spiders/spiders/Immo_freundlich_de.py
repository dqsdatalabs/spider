# -*- coding: utf-8 -*-
# Author: Adham Mansour
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class Immo_freundlichDeSpider(scrapy.Spider):
    name = 'Immo_freundlich_de'
    allowed_domains = ['immo-freundlich.de']
    start_urls = ['https://www.immo-freundlich.de/immobilien/?inx-search-distance-search-location=&inx-search-distance-search-radius=&inx-search-description=&inx-search-property-type=&inx-search-marketing-type=zu-vermieten&inx-search-locality=&inx-search-min-rooms=&inx-search-min-area=&inx-search-price-range=']  # https not http
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    current_page = 1
    position = 1
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
    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('a+ div a::attr(href)').extract()
        for rental in rentals:
            yield Request(url=rental,
                          callback=self.populate_item)
        if response.css('span~ a+ a'):
            self.current_page += 1
            yield Request(url=f'https://www.immo-freundlich.de/immobilien/page/{str(self.current_page)}/?inx-search-distance-search-location&inx-search-distance-search-radius&inx-search-description&inx-search-property-type&inx-search-marketing-type=zu-vermieten&inx-search-locality&inx-search-min-rooms&inx-search-min-area&inx-search-price-range',
                          callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        if not 'Gewerbeimmobilie' in response.css('.uk-width-auto\@s::text').extract_first():
            item_loader = ListingLoader(response=response)

            external_id = response.css('.inx-single-property__head-element-title::text').extract_first()
            title = response.css('h1::text').extract_first()
            description = ((((' '.join(response.css('.inx-description-text ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
            address = response.css('.uk-width-expand::text').extract_first()
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            property_type = 'apartment'
            list_items = response.css('.inx-detail-list__item')
            list_items_dict = {}
            for list_item in list_items:
                header = list_item.css('.inx-detail-list__title::text').extract_first()
                if header:
                    value = list_item.css('.inx-detail-list__value::text').extract_first()
                    if value:
                        list_items_dict[header] = value

            square_meters = None  # METERS #int(response.css('::text').extract_first())
            if 'Wohnfläche:' in list_items_dict.keys():
                square_meters = list_items_dict['Wohnfläche:']
                square_meters = square_meters.replace('\xa0','')
                square_meters = int(ceil(float(extract_number_only(square_meters))))

            room_count = 1  # int(response.css('::text').extract_first())
            if 'Zimmer insgesamt:' in list_items_dict.keys():
                room_count = list_items_dict['Zimmer insgesamt:']
                room_count = room_count.replace('\xa0','')
                room_count = int(ceil(float(extract_number_only(room_count))))

            bathroom_count = None  # int(response.css('::text').extract_first())
            if 'Badezimmer:' in list_items_dict.keys():
                bathroom_count = list_items_dict['Badezimmer:']
                bathroom_count = bathroom_count.replace('\xa0','')
                bathroom_count = int(ceil(float(extract_number_only(bathroom_count))))

            images = response.css('.inx-gallery__image img::attr(src)').extract()
            floor_plan_images = response.css('.inx-gallery__print-image img::attr(src)').extract()
            rent = None  # int(response.css('::text').extract_first())
            if 'Kaltmiete:' in list_items_dict.keys():
                rent = list_items_dict['Kaltmiete:']
                rent = rent.replace('\xa0','')
                rent = int(ceil(float(extract_number_only(rent))))

            deposit = None
            if 'monatl. Betriebs-/Nebenkosten:' in list_items_dict.keys():
                deposit = list_items_dict['monatl. Betriebs-/Nebenkosten:']
                deposit = deposit.replace('\xa0','')
                deposit = int(ceil(float(extract_number_only(deposit))))

            utilities = None
            if 'Kaution:' in list_items_dict.keys():
                utilities = list_items_dict['Kaution:']
                utilities = utilities.replace('\xa0','')
                utilities = int(ceil(float(extract_number_only(utilities))))

            energy_label = None
            if 'Badezimmer:' in list_items_dict.keys():
                energy_label = list_items_dict['Badezimmer:']

            floor = None  # response.css('::text').extract_first()
            if 'Etage:' in list_items_dict.keys():
                floor = list_items_dict['Etage:']
                floor = floor.replace('\xa0','')
                if floor == "0":
                    floor = None

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

            landlord_name = response.css('strong::text').extract()
            if landlord_name:
                landlord_name = landlord_name[-1]
            else:
                landlord_name = 'immo-freundlich'

            if 'E-Mail:' in list_items_dict.keys():
                landlord_email = list_items_dict['E-Mail:']
            else:
                landlord_email = 'groenewold@immo-freundlich.de'

            if 'Mobil:' in list_items_dict.keys():
                landlord_phone = list_items_dict['Mobil:']
            else:
                landlord_phone = '04954 – 937 07 62'
            description = ((((' '.join(response.css('.inx-single-property__section.inx-single-property__section--type--description-text .inx-description-text::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
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
