# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name

import requests
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, remove_white_spaces, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class MintoComSpider(scrapy.Spider):
    name = "minto_com"
    allowed_domains = ['minto.com']
    start_urls = ['https://www.minto.com/ottawa/apartment-rentals/projects.html']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_buildings)

    # 2. SCRAPING level 2
    def parse_buildings(self, response, **kwargs):
        buildings = response.css('.locname a::attr(href)').extract()
        for building in buildings:
            yield Request(url=building,
                          callback=self.parse_area_pages)

    # 3. SCRAPING level 3
    def parse_area_pages(self, response):
        if 'https://www.minto.com/' in response.url:
            amenities = (' '.join(response.css('#site-content .group li::text').extract())).lower()
            images = response.css('.locgallery > div > div > div > a::attr(href)').extract()
            description = remove_unicode_char((((' '.join(response.css('#overview p ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
            address = response.css('.text-md-left > h2 > a > span > span::text').extract()
            scrap_zipcode = address[-1]
            scrap_city = address[-3]
            scrap_address = address[0]
            address = ' '.join(address)
            title = response.css('.text-md-left h1::text').extract_first()
            landlord_phone = response.css('.text-md-center a::text').extract_first()

            longitude, latitude= extract_location_from_address(address)

            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            if not zipcode:
                zipcode =scrap_zipcode
            if not city:
                city =scrap_city
            if not address:
                address =scrap_address

            longitude = str(longitude)
            latitude = str(latitude)

            if 'pet' in amenities:
                pets_allowed = True
            else:
                pets_allowed = False

            if 'furnish' in amenities:
                furnished = True
            else:
                furnished = False

            if 'parking' in amenities:
                parking = True
            else:
                parking = False

            if 'elevator' in amenities:
                elevator = True
            else:
                elevator = False

            if 'balcony' in amenities:
                balcony = True
            else:
                balcony = False

            if 'terrace' in description.lower():
                terrace = True
            else:
                terrace = False

            if 'pool' in amenities:
                swimming_pool = True
            else:
                swimming_pool = False

            if 'dishwasher' in amenities:
                dishwasher = True
            else:
                dishwasher = False

            if 'laundry' in amenities:
                washing_machine = True
            else:
                washing_machine = False

            number_words = ['Zero','One', 'Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight','Nine', 'Ten']
            rentals = response.css('.py-md-0')
            counter =1
            for rental in rentals:
                rent = extract_number_only(rental.css('.price::text').extract_first())
                if rent != 0:
                    available_date = rental.css('.align-middle small::text').extract_first()
                    available_date = available_date.split(' ')
                    if len(available_date) == 3:
                        available_date = available_date[1] + '-' + available_date[-1]
                    elif len(available_date) == 4:
                        available_date = available_date[-1] + '-' +  available_date[1] + '-' + available_date[-2]
                    else:
                        available_date = ' '.join(available_date)
                    rental_title = rental.css('.text-left::text').extract_first()
                    rental_title = (remove_white_spaces(rental_title)).split(' ')
                    if rental_title[0] in number_words:
                        room_count = number_words.index((rental_title[0]).replace('/n',''))
                    else:
                        room_count = 1

                    # # MetaData
                    item_loader = ListingLoader(response=response)
                    item_loader.add_value("external_link", response.url+'#'+str(counter)) # String
                    item_loader.add_value("external_source", self.external_source) # String

                    #item_loader.add_value("external_id", external_id) # String
                    item_loader.add_value("position", self.position) # Int
                    item_loader.add_value("title", title) # String
                    item_loader.add_value("description", description) # String

                    # # Property Details
                    item_loader.add_value("city", city) # String
                    item_loader.add_value("zipcode", zipcode) # String
                    item_loader.add_value("address", address) # String
                    item_loader.add_value("latitude", latitude) # String
                    item_loader.add_value("longitude", longitude) # String
                    #item_loader.add_value("floor", floor) # String
                    item_loader.add_value("property_type", 'apartment') # String
                    #item_loader.add_value("square_meters", int(int(square_meters)*10.764))
                    item_loader.add_value("room_count", room_count) # Int
                    #item_loader.add_value("bathroom_count", bathroom_count) # Int

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
                    #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                    # # Monetary Status
                    item_loader.add_value("rent", int(rent)) # Int
                    #item_loader.add_value("deposit", deposit) # Int
                    #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                    #item_loader.add_value("utilities", utilities) # Int
                    item_loader.add_value("currency", "CAD") # String

                    #item_loader.add_value("water_cost", water_cost) # Int
                    #item_loader.add_value("heating_cost", heating_cost) # Int

                    #item_loader.add_value("energy_label", energy_label) # String

                    # # LandLord Details
                    item_loader.add_value("landlord_name", 'minto') # String
                    item_loader.add_value("landlord_phone", landlord_phone) # String
                    item_loader.add_value("landlord_email", 'infoline@minto.com') # String

                    self.position += 1
                    counter +=1
                    yield item_loader.load_item()
