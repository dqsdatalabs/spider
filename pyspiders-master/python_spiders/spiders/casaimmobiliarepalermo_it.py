# -*- coding: utf-8 -*-
# Author: Adham Mansour
import re

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_coordinates, \
    extract_location_from_address
from ..loaders import ListingLoader


class CasaimmobiliarepalermoItSpider(scrapy.Spider):
    name = 'casaimmobiliarepalermo_it'
    allowed_domains = ['casaimmobiliarepalermo.it']
    start_urls = ['http://www.casaimmobiliarepalermo.it/appartamenti-in-affitto/',
                  'http://www.casaimmobiliarepalermo.it/category/tipologia/villa/affitto-villa/']  # https not http
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            if 'appartamenti' in url:
                property_type = 'apartment'
            elif 'villa'  in url:
                property_type = 'house'
            yield scrapy.Request(url, callback=self.parse,meta={'property_type': property_type})

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('.botto::attr(href)').extract()
        for rental in rentals:
            yield Request(url=rental,
                          callback=self.populate_item,
                          meta={'property_type': response.meta['property_type']})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('.titolo::text').extract_first()
        description = remove_unicode_char((((' '.join(response.css('p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        map_script = response.css("script:contains('mapp.data.push')").get()

        content_li = response.css('#content li')
        for li in content_li:
            header_text = li.css('b::text').extract_first()
            if header_text == '€. ':
                rent = li.css('::text').extract()
                rent = int(extract_number_only(extract_number_only(rent[-1])))
            elif header_text =='Vani':
                room_count = li.css('::text').extract()
                room_count = int(extract_number_only(extract_number_only(room_count[-1])))
            elif header_text =='Piano':
                floor = li.css('::text').extract()
                floor = (floor[-1]).replace(':&nbsp','')
            elif header_text =='MQ':
                square_meters = li.css('::text').extract()
                square_meters = int(extract_number_only(extract_number_only(square_meters[-1])))
            elif header_text =='Zona':
                city = li.css('::text').extract()
                city = city[-1]

        address = city.replace(':&nbsp','')+ ', italy'



        if map_script is not None:
            lat_long = re.findall('{"lat":(-?\d*.?\d+),"lng":(-?\d*.?\d+)}."map',map_script)
            latitude = lat_long[0][0]
            longitude = lat_long[0][1]
            if latitude !='0':
                zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            else:
                longitude, latitude = extract_location_from_address(address)
                zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        else:
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        images = response.css('.gallery-icon img::attr(src)').extract()
        images = [re.sub('-*\d{3}x\d{3}','',i) for i in images]


        furnished = None
        if 'arredato' in description.lower():
            furnished = True

        parking = None
        if 'posto auto' in description.lower():
            parking = True

        balcony = None
        if 'balcon' in description.lower():
            balcony = True
        #
        terrace = None
        if 'terrazzini' in description.lower():
            terrace = True

        washing_machine = None
        if 'lavanderia' in description.lower():
            washing_machine = True

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
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
        item_loader.add_value("property_type", response.meta['property_type']) # String
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

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
        item_loader.add_value("landlord_name", 'Sofia Immobiliare S.r.') # String
        item_loader.add_value("landlord_phone", '091.336456') # String
        item_loader.add_value("landlord_email", 'info@casaimmobiliarepalermo.it') # String

        self.position += 1
        yield item_loader.load_item()
