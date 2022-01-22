# -*- coding: utf-8 -*-
# Author: Adham Mansour
import re

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class ApartmentswestDeSpider(scrapy.Spider):
    name = 'apartmentswest_de'
    allowed_domains = ['apartments-west.de']
    start_urls = ['https://www.apartments-west.de/apartmentuebersicht/#hidden_image_id']
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.n_apartment_parse)

    def n_apartment_parse(self, response):
        n_apartments = response.css('.aw_apartment_overview .aw_more_information a::attr(href)').extract()
        for rooms, apartment in (enumerate(n_apartments, 1)):
            yield Request(url=apartment,
                          callback=self.parse,
                          meta={'room_count': rooms})

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('.aw_vertical_align_middle_overview .aw_apartment_overview')
        for rental in rentals:
            occupied = rental.css('.aw_belegung_uebersicht span p ::text').extract_first()
            if occupied: occupied = occupied.lower()
            else: occupied = ''
            if ('belegt' not in occupied):
                available_date = str(extract_number_only(occupied))
                if available_date !='0':
                    available_date = (available_date[4:]+'-'+available_date[2:4]+'-'+available_date[:2])
                else:
                    available_date = None
                rental_link = rental.css('.aw_more_information span a::attr(href)').extract_first()
                yield Request(url=rental_link,
                              callback=self.populate_item,
                              meta={'room_count': response.meta['room_count'],
                                    'available_date' : available_date})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = (response.css('#x-content-band-1 .h4 ::text').extract())
        title = [(remove_unicode_char(i)).replace('\n','') for i in title]
        title = [i for i in title if i]
        square_meters = title[-1]
        title = " ".join(title[:-2])
        description = remove_unicode_char((((' '.join(response.css('#x-content-band-2 p::text').extract()).replace('\n','')).replace('\t', '')).replace('\r', '')))
        address = 'Asset management Sawhney Ludwigstrasse 86 A DE - 70197 Stuttgart'
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        property_type = 'apartment'
        square_meters = extract_number_only(square_meters)
        room_count = response.meta['room_count']
        bathroom_count = 1  # int(response.css('::text').extract_first())

        images = response.css('.rev_slider img::attr(src)').extract()
        floor_plan_images = response.css('#hidden_image_id::attr(src)').extract()
        monetary_para = ' '.join(response.css('#x-content-band-3 .h4+ p::text').extract())
        rent = extract_number_only(re.findall('([\d|\.]+).- € ab 3 Monaten Mietzeit,', monetary_para))
        if rent == 0:
            rent = extract_number_only(re.findall('Kaltmiete ([\d|\.]+)', monetary_para))
        deposit = extract_number_only(re.findall('aution ([\d|\.]+)-', monetary_para))
        terrace_or_balcony = (response.css('#x-content-band-1 .h4+ p::text').extract_first())
        terrace = None
        balcony = None
        if terrace_or_balcony:
            if 'terrasse' in (terrace_or_balcony).lower():
                terrace = True
            elif 'balkon' in (terrace_or_balcony).lower():
                balcony = True
            else:
                terrace = None
                balcony = None
        else:
            terrace = ''

        washing_machine = None
        if 'waschmaschine' in description.lower():
            washing_machine = True
        else:
            washing_machine = False

        dishwasher = None
        if 'geschirrsp' in description.lower():
            dishwasher = True
        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        # item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type)  # String
        item_loader.add_value("square_meters", int(square_meters))  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", response.meta['available_date']) # String => date_format also "Available", "Available Now" ARE allowed

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        # item_loader.add_value("parking", parking) # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # # Monetary Status
        item_loader.add_value("rent", int(rent))  # Int
        item_loader.add_value("deposit", int(deposit))  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'apartments west')  # String
        item_loader.add_value("landlord_phone", '+49 (0) 711. 566 11 9 00')  # String
        item_loader.add_value("landlord_email", 'contact@apartments-west.com')  # String

        self.position += 1
        yield item_loader.load_item()
