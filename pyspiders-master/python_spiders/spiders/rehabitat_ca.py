import json
import re

import scrapy
from scrapy import Request

from ..helper import remove_white_spaces, extract_number_only, sq_feet_to_meters
from ..loaders import ListingLoader


class RehabitatCaSpider(scrapy.Spider):
    name = 'rehabitat_ca'
    allowed_domains = ['rehabitat.ca']
    start_urls = ['https://api.theliftsystem.com/v2/search?locale=en&client_id=1081&auth_token=sswpREkUtyeYjeoahA2i&city_id=1485&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=1800&min_sqft=0&max_sqft=100000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=1863%2C1098%2C33066%2C1484%2C3218%2C3326%2C1485&pet_friendly=&offset=0&count=false',
                  ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'


    def start_requests(self):

        yield Request(url=self.start_urls[0],
                      callback=self.parse,
                      method='GET'
        )

    def parse(self, response, requests=None):
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            external_link = item['permalink']
            external_link = external_link.split('/')
            external_link = external_link[-1]
            external_link = 'https://www.rehabitat.ca/apartments/' + external_link
            external_id = item['id']
            external_source = self.external_source
            title = item['website']['title']
            if title is None:
                title = item['name']
            description = item['website']['description']
            city = item['address']['city']
            zipcode = item['address']['postal_code']
            address = item['address']['address']
            latitude = item['geocode']['latitude']
            longitude = item['geocode']['longitude']
            property_type = item['property_type']
            property_type = property_type.split('-')
            property_type = property_type[-1]
            property_type = property_type.lower()
            if property_type == 'home':
                property_type = 'house'
            else:
                property_type = 'apartment'

            square_meters = int(float(item['statistics']['suites']['square_feet']['max']))
            if square_meters == 0:
                square_meters = 1


            available_date = item['availability_status_label']


            pets_allowed = item['pet_friendly']


            parking = item['parking']
            if (parking['additional'] or parking['indoor'] or parking['outdoor']) != 'null':
                parking = True
            else:
                parking = False
            landlord_name = item['contact']['name']
            landlord_email = item['contact']['email']
            landlord_phone = item['contact']['phone']
            yield Request(url=external_link,
                          callback=self.html_parse,
                          meta={
                              'external_link' : external_link,
                              'external_id' : external_id,
                              'external_source' : external_source,
                              'title' : title,
                              "description" : description,
                              'city' : city,
                              'zipcode' :zipcode,
                              'address': address,
                              'latitude' : latitude,
                              'property_type' : property_type,
                              'square_meters': square_meters,
                              'available_date': available_date,
                              'pets_allowed': pets_allowed,
                              'parking': parking,
                              'landlord_name': landlord_name,
                              'landlord_email': landlord_email,
                              'landlord_phone': landlord_phone,
                              'longitude': longitude,
                          }
                          )
    def html_parse(self,response):
        # --------------------------------#
        # item loaders
        listings = response.css('.suite-info-container')
        description= (" ".join(response.css('.main > p ::text').extract())).lower()
        if description is None:
            description = response.meta['description']
        description.lower()
        for i in listings:
            uni_images = response.css('#slickslider-default-id-0 a::attr(href)').extract()
            info_blocks_dict = {}
            info_blocks = i.css('.info-block')
            for j in info_blocks:
                label = remove_white_spaces((j.css('.label::text').extract_first()).replace('\n',''))
                if label == "Suite Photos" or label == "Floorplans":
                    info = j.css('.info a::attr(href)').extract()
                    info_blocks_dict[label] = info
                else:
                    info = remove_white_spaces((j.css('.info::text').extract_first()).replace('\n', ''))
                    info_blocks_dict[label] = info
            if 'Rent' in info_blocks_dict:
                amenity = response.css('.amenity-holder::text').extract()
                amenity = [remove_white_spaces(i) for i in amenity]
                amenity = [i for i in amenity if i]
                amenity = [i.lower() for i in amenity]
                if 'Bedrooms' in info_blocks_dict:
                    room_count = int(info_blocks_dict['Bedrooms'])
                    if room_count == 0:
                        room_count = 1
                else:
                    room_count = 1





                if 'Bathrooms' in info_blocks_dict:
                    bathroom_count = int(info_blocks_dict['Bathrooms'])
                    if bathroom_count == 0:
                        bathroom_count = 1
                else:
                    bathroom_count = 1

                if 'Suite Photos' in info_blocks_dict:
                    uni_images.extend(info_blocks_dict['Suite Photos'])
                images = uni_images

                if 'Floorplans' in info_blocks_dict:
                    floor_plan_images = info_blocks_dict['Floorplans']
                else:
                    floor_plan_images = None
                external_images_count = len(images)
                rent = int(extract_number_only(extract_number_only(info_blocks_dict['Rent'])))
                if 'Square feet' in info_blocks_dict:
                    square_meters = sq_feet_to_meters(info_blocks_dict['Square feet'])
                else:
                    square_meters = 1

                currency = "CAD"
                deposit = None
                prepaid_rent = None
                utilities = None
                water_cost = None
                heating_cost = None
                energy_label = None
                if 'furnished' in amenity:
                    furnished = True
                else:
                    furnished = False
                floor = None
                elevator = None

                if 'balcony' in (description):
                    balcony = True
                else:
                    balcony = False

                terrace = None
                swimming_pool = None
                if ' washer' in description:
                    washing_machine = True
                else:
                    washing_machine = False

                if ' dishwasher' in description:
                    dishwasher = True
                else:
                    dishwasher = False
                item_loader = ListingLoader(response=response)
                item_loader.add_value('external_link', response.meta['external_link'])
                item_loader.add_value('external_id', str(response.meta['external_id']))
                item_loader.add_value('external_source', response.meta['external_source'])
                item_loader.add_value('title', response.meta['title'])
                item_loader.add_value('description', description)
                item_loader.add_value('city', response.meta['city'])
                item_loader.add_value('zipcode', response.meta['zipcode'])
                item_loader.add_value('address', response.meta['address'])
                item_loader.add_value("latitude", str(response.meta['latitude']))
                item_loader.add_value("longitude", str(response.meta['longitude']))
                item_loader.add_value('property_type', response.meta['property_type'])
                item_loader.add_value('square_meters', int(int(square_meters)*10.764))
                item_loader.add_value('room_count', room_count)
                item_loader.add_value('bathroom_count', bathroom_count)
                item_loader.add_value('available_date', response.meta['available_date'])
                item_loader.add_value("images", images)
                item_loader.add_value("floor_plan_images", floor_plan_images)
                item_loader.add_value("external_images_count", external_images_count)
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", currency)
                item_loader.add_value("deposit", deposit)
                item_loader.add_value("prepaid_rent", prepaid_rent)
                item_loader.add_value("utilities", utilities)
                item_loader.add_value("water_cost", water_cost)
                item_loader.add_value("heating_cost", heating_cost)
                item_loader.add_value("energy_label", energy_label)
                item_loader.add_value("pets_allowed", response.meta['pets_allowed'])
                item_loader.add_value("furnished", furnished)
                item_loader.add_value("floor", floor)
                item_loader.add_value("parking", response.meta['parking'])
                item_loader.add_value("elevator", elevator)
                item_loader.add_value("balcony", balcony)
                item_loader.add_value("terrace", terrace)
                item_loader.add_value("swimming_pool", swimming_pool)
                item_loader.add_value("washing_machine", washing_machine)
                item_loader.add_value("dishwasher", dishwasher)
                item_loader.add_value("landlord_name", response.meta['landlord_name'])
                item_loader.add_value("landlord_email", response.meta['landlord_email'])
                item_loader.add_value("landlord_phone", response.meta['landlord_phone'])

                yield item_loader.load_item()
