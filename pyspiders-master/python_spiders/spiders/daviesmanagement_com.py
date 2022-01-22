# -*- coding: utf-8 -*-
# Author: Adham Mansour
import json
import re
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, remove_white_spaces, extract_location_from_coordinates
from ..loaders import ListingLoader


class DaviesmanagementComSpider(scrapy.Spider):
    name = 'daviesmanagement_com'
    allowed_domains = ['daviesmanagement.com','api.theliftsystem.com']
    start_urls = ['https://api.theliftsystem.com/v2/search?locale=en&client_id=125&auth_token=sswpREkUtyeYjeoahA2i&city_id=370&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=100000&min_sqft=0&max_sqft=100000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=max_rate+DESC&limit=1000&neighbourhood=&amenities=&promotions=&city_ids=845%2C370&pet_friendly=&offset=0&count=false']  # https not http
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

    def parse(self, response):
        parsed_response = json.loads(response.body)
        for building in parsed_response:
            address = building['address']['address']
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
            square_meters = building['statistics']['suites']['square_feet']['min']
            if square_meters:
                square_meters = int(ceil(float(square_meters)))
                if square_meters == 0:
                    square_meters = None
            landlord_email = building['client']['email']
            landlord_name = building['client']['name']
            if landlord_name == "":
                landlord_name = 'Davies Property Management'
            title = building['website']['title']
            if title is None or title == '':
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
                              'square_meters'  : square_meters,
                              'address' : address
                            })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        description = remove_unicode_char((((' '.join(response.css('.suite-info p::text , .main p::text , .description li::text').extract()).replace('\n', '')).replace('\t', '')).replace('\r', '')))
        description = description.replace('Leave This Blank  Captcha:','')
        description = description.replace('To book a time to see this unit, please call 587-605-1486', '')
        description = description.replace('Photographs are for advertising purposes only and may not be of the actual suite available.', '')
        latitude = response.meta['latitude']
        longitude = response.meta['longitude']
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        address = response.meta['address']
        images = response.css('.gallery-image::attr(href)').extract()
        amenities = remove_unicode_char((((' '.join((response.css('.amenity-group p::text , .main li::text , .amenity-holder::text').extract()))).replace('\n','')).replace(';','')).replace('\t','')).replace('\r', '')

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

        allrent = (response.css('.suite-rate::text').extract())
        allroom_count = (response.css('.info-block:nth-child(1) .info::text').extract())
        allbathroom_count = (response.css('.info-block:nth-child(2) .info::text').extract())
        allavailable_date = response.css('.accessible-modal::text').extract()

        info_block = response.css('.info-block')
        for i in info_block:
            header = i.css('.label::text').extract_first()
            if 'Suite Photos' in header:
                extra_images = i.css('.info a::attr(href)').extract()
                images = images + extra_images
        counter = 1
        for i in range(len(allroom_count)):
            available_date = allavailable_date[i]

            if available_date:
                if available_date != 'Available Now':
                    available_date = re.findall('([\w]+)\. ([\d]+), ([\d]+)',available_date)
                    if available_date:
                        available_date = available_date[0][-1]+'-'+available_date[0][0]+'-'+available_date[0][1]
                else:
                    available_date = 'Available Now'
            else:
                available_date = None

            bathroom_count = int(ceil(float(allbathroom_count[i])))

            room_count = int(ceil(float(allroom_count[i])))
            if room_count == 0:
                room_count =1

            rent = int(extract_number_only(extract_number_only(allrent[i])))
            if rent != 0:
                # # MetaData
                item_loader = ListingLoader(response=response)
                item_loader.add_value("external_link", response.url + "#" + str(counter))  # String
                item_loader.add_value("external_source", self.external_source)  # String

                item_loader.add_value("external_id", str(response.meta['external_id']))  # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", response.meta['title'])  # String
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