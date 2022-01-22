import re

import scrapy
from scrapy import Request

# Author: Adham Mansour
# -*- coding: utf-8 -*-
from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates, remove_white_spaces
from ..loaders import ListingLoader


class ConcertpropertiesComSpider(scrapy.Spider):
    name = 'concertproperties_com'
    allowed_domains = ['concertproperties.com']
    start_urls = ['https://www.concertproperties.com/rentals']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.n_city_parse)

    def n_city_parse(self, response):
        n_city = response.css('.alt-link::attr(href)').extract()
        for city in n_city:
            yield Request(url=city,
                          callback=self.parse,
                          )

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('.card--square')
        for rental in rentals:
            rental_link = 'https://www.concertproperties.com' + rental.css('.card__link::attr(href)').extract_first()
            yield Request(url=rental_link,
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.css('h1::text').extract_first()
        description = remove_unicode_char((((' '.join(response.css('.field-field-description p ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        address = ' '.join(response.css('.wysiwyg ::text').extract())
        address = remove_white_spaces(address)
        if address:
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        property_type = 'apartment'
        images = response.css('.views-field-field-rental-gallery-fid img::attr(src)').extract()
        amenities = ' '.join(response.css('.item-list li::text').extract())

        apartments = response.css('tr')
        for n,apartment in enumerate(apartments,1):
            rent = apartment.css('td.active::text').extract_first()
            if rent:
                exteral_link = response.url +'#'+str(n)
                item_loader = ListingLoader(response=response)
                rent = rent.split('-')
                rent = int(extract_number_only(extract_number_only(rent[0])))
                room_count = remove_white_spaces(apartment.css('.views-field-tid::text').extract_first())
                if room_count == 'Studio':
                    property_type = 'studio'
                    room_count = 1
                elif room_count[0].isnumeric():
                    room_count = int(room_count[0])
                balcony = None
                if 'balcony' in description.lower():
                    balcony = True

                terrace = None
                if 'terrace' in amenities.lower():
                    terrace = True


                washing_machine = None
                if 'laundry' in amenities.lower():
                    washing_machine = True
                # # MetaData
                item_loader.add_value("external_link", exteral_link)  # String
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
                # item_loader.add_value("floor", floor) # String
                item_loader.add_value("property_type", property_type) # String
                # item_loader.add_value("square_meters", square_meters) # Int
                item_loader.add_value("room_count", room_count) # Int
                # item_loader.add_value("bathroom_count", bathroom_count) # Int

                # item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

                # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                # item_loader.add_value("furnished", furnished) # Boolean
                # item_loader.add_value("parking", parking) # Boolean
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
                item_loader.add_value("currency", "CAD") # String

                # item_loader.add_value("water_cost", water_cost) # Int
                # item_loader.add_value("heating_cost", heating_cost) # Int

                # item_loader.add_value("energy_label", energy_label) # String

                # # LandLord Details
                item_loader.add_value("landlord_name", 'concert properties') # String
                item_loader.add_value("landlord_phone", '416.639.1103') # String
                item_loader.add_value("landlord_email", 'WebGeneral@ConcertProperties.com') # String

                self.position += 1
                yield item_loader.load_item()
