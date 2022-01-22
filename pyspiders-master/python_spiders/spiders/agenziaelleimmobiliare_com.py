# -*- coding: utf-8 -*-
# Author: Adham Mansour
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_coordinates
from ..loaders import ListingLoader


class AgenziaelleimmobiliareComSpider(scrapy.Spider):
    name = 'agenziaelleimmobiliare_com'
    allowed_domains = ['agenziaelleimmobiliare.com']
    start_urls = ['https://www.agenziaelleimmobiliare.com/it/immobili?contratto=2']  # https not http
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
        rentals = response.css('.property-thumb-info-content a::attr(href)').extract()
        for rental in rentals:
            yield Request(url='https://www.agenziaelleimmobiliare.com/'+rental,
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        list_items = response.css('.amenities-detail li')
        list_items_dict = {}
        for list_item in list_items:
            headers = list_item.css('::text').extract()
            list_items_dict[headers[0]] = headers[1]
        external_id = None # response.css('::text').extract_first()
        if 'Rif.:' in list_items_dict.keys():
            external_id = list_items_dict['Rif.:']
        title = response.css('.page-top-in span::text').extract_first()
        description = ((((' '.join(response.css('.col-sm-8 p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        metas= response.css('meta')
        lat_lng = None
        for meta in metas:
            if meta.css('::attr(name)').extract_first() == 'geo.position':
                lat_lng = (meta.css('::attr(content)').extract_first())
        latitude = None
        longitude = None
        if lat_lng:
            lat_lng = lat_lng.split(';')
            latitude = lat_lng[0]
            longitude = lat_lng[1]
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        property_type = 'apartment'

        square_meters = None  # METERS #int(response.css('::text').extract_first())
        if 'Superficie:' in list_items_dict.keys():
            square_meters = list_items_dict['Superficie:']
            square_meters = int(ceil(float(extract_number_only(square_meters))))

        room_count = None  # int(response.css('::text').extract_first())
        if 'Locali/vani:' in list_items_dict.keys():
            room_count = list_items_dict['Locali/vani:']
            room_count = int(ceil(float(extract_number_only(room_count))))

        bathroom_count = None  # int(response.css('::text').extract_first())
        if 'Bagni:' in list_items_dict.keys():
            bathroom_count = list_items_dict['Bagni:']
            bathroom_count = int(ceil(float(extract_number_only(bathroom_count))))


        images = response.css('#carousel img::attr(src)').extract()

        rent = None  # int(response.css('::text').extract_first())
        if 'Prezzo:' in list_items_dict.keys():
            rent = list_items_dict['Prezzo:']
            rent = int(ceil(float(extract_number_only(rent))))

        energy_label = None
        if 'Classe energ.:' in list_items_dict.keys():
            energy_label = list_items_dict['Classe energ.:']

        floor = None  # response.css('::text').extract_first()
        if 'Piano:' in list_items_dict.keys():
            floor = list_items_dict['Piano:']

        pets_allowed = None
        if any(word in remove_unicode_char(description.lower()) for word in self.keywords['pets_allowed']):
            pets_allowed = True

        furnished = None
        if 'Arredato:' in list_items_dict.keys():
            furnished = list_items_dict['Arredato:']
            if furnished.lower() == 'arredato':
                furnished = True
            elif furnished.lower() == 'non arredato':
                furnished = False
            else:
                furnished = None
        #
        parking = None
        if 'Posti auto:' in list_items_dict.keys():
            parking = True
        #
        elevator = None
        if 'Ascensore:' in list_items_dict.keys():
            elevator = list_items_dict['Ascensore:']
            if elevator.lower() == 'si':
                elevator = True
            elif elevator.lower() == 'non':
                elevator = False
            else:
                elevator = None
        balcony = None
        if 'Balconi:' in list_items_dict.keys():
            balcony = list_items_dict['Balconi:']
            if balcony.lower() == 'si':
                balcony = True
            elif balcony.lower() == 'non':
                balcony = False
            else:
                balcony = None
        #
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

        landlord_name = 'agenzia elle immobiliare'
        landlord_email = 'info@agenziaelleimmobiliare.com'
        landlord_phone = '+39 3341319909'

        # # MetaData
        if rent !=0:
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

            item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
