# -*- coding: utf-8 -*-
# Author: Adham Mansour
import json
import re
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, remove_white_spaces, extract_location_from_coordinates
from ..loaders import ListingLoader


class PacificcovepropertiesComSpider(scrapy.Spider):
    name = 'pacificcoveproperties_com'
    allowed_domains = ['pacificcoveproperties.com','api.theliftsystem.com']
    start_urls = ['https://www.pacificcoveproperties.com/']  # https not http
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        city_ids = response.css('.homesearch-data::attr(data-city-ids)').extract_first()
        city_ids = city_ids.split(',')
        for city_id in city_ids:
                yield Request(url=f'https://api.theliftsystem.com/v2/search?show_custom_fields=true&client_id=152&auth_token=sswpREkUtyeYjeoahA2i&city_id={city_id}',
                              callback=self.apiparse)

    def apiparse(self, response):
        parsed_response = json.loads(response.body)
        for building in parsed_response:
            if building['parking']['additional'] == "" and building['parking']['indoor'] == "" and building['parking']['outdoor'] == "":
                parking = False
            else:
                parking = True
            external_link = building['permalink']
            external_id = building['id']
            property_type = building['property_type']
            if 'home' in property_type or 'house' in property_type:
                property_type = 'house'
            else:
                property_type = 'apartment'
            pets_allowed = building['pet_friendly']
            if pets_allowed == 'n/a':
                pets_allowed = None
            square_meters = building['statistics']['suites']['square_feet']['average']
            if square_meters:
                square_meters = int(ceil(float(square_meters)))
            elif square_meters == 0:
                square_meters = None
            landlord_email = building['contact']['email']
            landlord_email = landlord_email.split(',')
            landlord_email = landlord_email[0]
            landlord_name = building['contact']['name']
            if landlord_name == "":
                landlord_name = 'Pacific Cove Properties'
            title = building['website']['title']
            if title is None:
                title = building['name']
            yield Request(url=external_link,
                          callback=self.populate_item,
                          meta={
                            "landlord_email":landlord_email,
                            'landlord_phone' : building['contact']['phone'],
                            'landlord_name' : landlord_name,
                            'description' : building['details']['overview'],
                            'latitude' : building['geocode']['latitude'],
                            'longitude' :  building['geocode']['longitude'],
                            'title' : title,
                            'parking' : parking,
                            'pets_allowed' : pets_allowed,
                            'property_type' : property_type,
                            'external_id' : external_id,
                              'square_meters'  : square_meters
                            })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        rentaltitle = remove_white_spaces(response.css('.modal-building-name::text').extract_first())
        description = remove_unicode_char((((' '.join(response.css('.property-description p::text').extract()).replace('\n', '')).replace('\t', '')).replace('\r', '')))
        latitude = response.meta['latitude']
        longitude = response.meta['longitude']
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        images = response.css('.slides img::attr(src)').extract()

        amenities = remove_unicode_char((((' '.join(response.css('.amenities li::text').extract()).replace('\n','')).replace('\t','')).replace('\r', '')))

        furnished = None
        if 'furnish' in description.lower() or 'furnish' in amenities.lower():
            furnished = True

        elevator = None
        if 'elevator' in description.lower() or 'elevator' in amenities.lower():
            elevator = True

        balcony = None
        if 'balcon' in description.lower() or 'balcon' in amenities.lower():
            balcony = True

        terrace = None
        if 'terrace' in description.lower() or 'terrace' in amenities.lower():
            terrace = True

        swimming_pool = None
        if 'pool' in description.lower() or 'pool' in amenities.lower():
            swimming_pool = True

        washing_machine = None
        if 'laundry' in description.lower() or 'laundry' in amenities.lower():
            washing_machine = True

        parking = None
        if 'parking' in description.lower() or 'parking' in amenities.lower():
            parking = True

        dishwasher = None
        if 'dishwasher' in description.lower() or 'dishwasher' in amenities.lower():
            dishwasher = True
        rentals = response.css('.suite-available')
        counter = 1
        for rental in rentals:
            available_date = rental.css('.availability::text').extract_first()
            if available_date:
                available_date = re.findall('([\w]+)\. ([\d]+), ([\d]+)',available_date)
                if available_date:
                    available_date = available_date[0][-1]+'-'+available_date[0][0]+'-'+available_date[0][1]
            else:
                available_date = None
            title = rental.css('.suite-type::text').extract_first()
            bed_bath = re.findall('(\d) Bed', title)
            try:
                room_count = bed_bath[0][0]
            except:
                room_count = 1
            bathroom_count = (extract_number_only(rental.css('.baths .value::text').extract_first()))
            rent = int(extract_number_only(rental.css('.bottom-right .value::text').extract_first()))
            if rent != 0:
                # # MetaData
                item_loader = ListingLoader(response=response)
                item_loader.add_value("external_link", response.url + "#" + str(counter))  # String
                item_loader.add_value("external_source", self.external_source)  # String

                item_loader.add_value("external_id", str(response.meta['external_id']))  # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", rentaltitle)  # String
                item_loader.add_value("description", description)  # String

                # # Property Details
                item_loader.add_value("city", city)  # String
                item_loader.add_value("zipcode", zipcode)  # String
                item_loader.add_value("address", address)  # String
                item_loader.add_value("latitude", latitude)  # String
                item_loader.add_value("longitude", longitude)  # String
                # item_loader.add_value("floor", floor) # String
                item_loader.add_value("property_type", response.meta['property_type'])  # String
                item_loader.add_value("square_meters", response.meta['square_meters'])  # Int
                item_loader.add_value("room_count", room_count)  # Int
                item_loader.add_value("bathroom_count", bathroom_count)  # Int

                item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

                item_loader.add_value("pets_allowed", response.meta['pets_allowed'])  # Boolean
                item_loader.add_value("furnished", furnished)  # Boolean
                item_loader.add_value("parking", parking)  # Boolean
                item_loader.add_value("elevator", elevator)  # Boolean
                item_loader.add_value("balcony", balcony)  # Boolean
                item_loader.add_value("terrace", terrace)  # Boolean
                item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
                item_loader.add_value("washing_machine", washing_machine)  # Boolean
                item_loader.add_value("dishwasher", dishwasher)  # Boolean

                # # Images
                item_loader.add_value("images", images)  # Array
                item_loader.add_value("external_images_count", len(images))  # Int
                # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                # # Monetary Status
                item_loader.add_value("rent", rent)  # Int
                # item_loader.add_value("deposit", deposit)  # Int
                item_loader.add_value("currency", "CAD")  # String

                # # LandLord Details
                item_loader.add_value("landlord_name", response.meta['landlord_name'])  # String
                item_loader.add_value("landlord_phone", response.meta['landlord_phone'])  # String
                item_loader.add_value("landlord_email", response.meta['landlord_email'])  # String

                counter += 1
                self.position += 1
                yield item_loader.load_item()
