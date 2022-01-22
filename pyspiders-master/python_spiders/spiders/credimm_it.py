# -*- coding: utf-8 -*-
# Author: Adham Mansour
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, remove_white_spaces, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class CredimmItSpider(scrapy.Spider):
    name = 'credimm_it'
    allowed_domains = ['credimm.it']
    start_urls = ['https://www.credimm.it/immobili/?status=appartamenti-in-affitto&type=appartamento',
                  'https://www.credimm.it/immobili/?status=appartamenti-in-affitto&type=casa-indipendente',
                  'https://www.credimm.it/immobili/?status=appartamenti-in-affitto&type=villa'
                  ]  # https not http
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    keywords = {
        'pets_allowed': ['domestici','domestici','animali ammessi'],
        'furnished': ['arredato'],
        'parking': ['parcheggio','parco','box auto','autorimessa'],
        'elevator': ['ascensore', 'sollevamento', 'passaggio', 'portanza', 'strappo', 'montacarichi'],
        'balcony': ['balcone',' balconata'],
        'terrace': ['terrazza','terrazzo'],
        'swimming_pool': ['piscina','pool'],
        'washing_machine': ['lavatrice','rondella','rondello'],
        'dishwasher': ['lavastoviglie','lavapiatti']
    }
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        print('inside parse_area')
        rentals = response.css('.property-item')
        for rental in rentals:
            external_link = rental.css('h4 a::attr(href)').extract_first()
            yield Request(url=external_link,
                          callback=self.populate_item)
        if response.css('.current::text').extract_first() != response.css('.real-btn:nth-last-child(1)::text').extract_first():
            current = response.css('.current::text').extract_first()
            last = response.css('.real-btn:nth-last-child(1)::text').extract_first()
            print(current,last)
            external_link = ((response.url)[:-1])+'/page/'+str(int(current)+1)+'/'
            print(external_link)
            if current == '1':
                yield Request(url=((response.url)[:-1])+'/page/'+str(int(current)+1)+'/',
                              callback=self.parse)
            else:
                yield Request(url=((response.url)[:-2])+str(int(current)+1)+'/',
                              callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        external_id = remove_white_spaces(((response.css('#overview .title::text').extract_first()).split(': '))[1])
        title = response.css('.page-title span ::text').extract_first()
        description = ((((' '.join(response.css('#overview p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        print(title)
        address = ((title.split('–'))[1]) + ', Italy' # response.css('::text').extract_first()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        property_type = 'apartment'
        if 'casa' in title.lower() or 'villa' in title.lower():
            property_type = 'house'
        square_meters = int(ceil(float(extract_number_only(remove_unicode_char(response.css('.property-meta span:nth-child(1)::text').extract_first())))))

        room_count = extract_number_only(response.css('.property-meta span:nth-child(2)::text').extract_first())
        bathroom_count = extract_number_only(response.css('.property-meta span:nth-child(3)::text').extract_first())

        images = response.css('.slides img::attr(src)').extract()
        rent = int(ceil(float(extract_number_only(response.css('.status-label+ span::text').extract_first()))))

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

        landlord_name = 'Credimm Servizi Immobiliari'
        landlord_email = 'info@credimm.it'
        landlord_phone = '0803960636'

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
        # item_loader.add_value("floor", floor) # String
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
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
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
