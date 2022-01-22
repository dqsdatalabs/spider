import json
import re

import scrapy
from scrapy import Request

from ..helper import remove_white_spaces, extract_number_only, sq_feet_to_meters
from ..loaders import ListingLoader


class FamilypropertiesCaSpider(scrapy.Spider):
    name = 'familyproperties_ca'
    allowed_domains = ['familyproperties.ca']
    start_urls = ['https://api.theliftsystem.com/v2/search?client_id=286&auth_token=sswpREkUtyeYjeoahA2i&city_id=831&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1400&max_rate=2200&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=max_rate+ASC%2C+min_rate+ASC%2C+min_bed+ASC%2C+max_bath+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
                  'https://api.theliftsystem.com/v2/search?client_id=286&auth_token=sswpREkUtyeYjeoahA2i&city_id=1154&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=2200&max_rate=2300&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=max_rate+ASC%2C+min_rate+ASC%2C+min_bed+ASC%2C+max_bath+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
                  'https://api.theliftsystem.com/v2/search?client_id=286&auth_token=sswpREkUtyeYjeoahA2i&city_id=1174&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=800&max_rate=1600&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=max_rate+ASC%2C+min_rate+ASC%2C+min_bed+ASC%2C+max_bath+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
                  'https://api.theliftsystem.com/v2/search?client_id=286&auth_token=sswpREkUtyeYjeoahA2i&city_id=2081&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1700&max_rate=2200&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=max_rate+ASC%2C+min_rate+ASC%2C+min_bed+ASC%2C+max_bath+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
                  'https://api.theliftsystem.com/v2/search?client_id=286&auth_token=sswpREkUtyeYjeoahA2i&city_id=2559&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=900&max_rate=1500&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=max_rate+ASC%2C+min_rate+ASC%2C+min_bed+ASC%2C+max_bath+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
                  'https://api.theliftsystem.com/v2/search?client_id=286&auth_token=sswpREkUtyeYjeoahA2i&city_id=3133&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=800&max_rate=2700&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=max_rate+ASC%2C+min_rate+ASC%2C+min_bed+ASC%2C+max_bath+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false'
                  ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'


    def start_requests(self):
        header = {
            'Host' : 'api.theliftsystem.com',
            'Connection' : 'keep-alive',
            'sec-ch-ua' : '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
            'Accept' : 'application/json, text/javascript, */*; q=0.01',
            'User-Agent': 'Mozilla / 5.0(Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
            'sec-ch-ua-platform': "Windows",
            'Origin': 'https://www.familyproperties.ca',
            'Referer': 'https://www.familyproperties.ca/'

        }
        for i in self.start_urls:
            yield Request(url=i,
                        callback=self.parse,
                        method='GET',
                        headers=header
            )

    def parse(self, response, requests=None):
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            external_link = item['permalink']
            external_link = external_link.split('/')
            external_link = external_link[-1]
            external_link = 'https://www.familyproperties.ca/apartments/' + external_link
            external_id = item['id']
            external_source = self.external_source
            title = item['website']['title']
            if title is None:
                title = item['name']
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

            photos = [item['photo_path']]


            pets_allowed = item['pet_friendly']
            if pets_allowed == "true":
                pets_allowed = True
            else:
                pets_allowed = False

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
                              'city' : city,
                              'zipcode' :zipcode,
                              'address': address,
                              'latitude' : latitude,
                              'property_type' : property_type,
                              'pets_allowed': pets_allowed,
                              'landlord_name': landlord_name,
                              'landlord_email': landlord_email,
                              'landlord_phone': landlord_phone,
                              'longitude': longitude,
                              'photos' : photos
                          }
                          )
    def html_parse(self,response):
        listings = response.css('.table')
        description= (" ".join(response.css('.cms-content p ::text').extract())).lower()
        description.lower()
        amenity = response.css('.amenity-holder::text').extract()
        amenity = [remove_white_spaces(i) for i in amenity]
        amenity = [i for i in amenity if i]
        amenity = (" ".join(amenity)).lower()
        for i in listings:
            square_meters = extract_number_only(i.css('.sq-ft::text').extract_first())
            rent = extract_number_only(i.css('.rate-value::text').extract_first())
            if rent != '':
                room_count = extract_number_only(i.css('.type-name::text').extract_first())
                if room_count == 0:
                    room_count = 1
                available_date = remove_white_spaces(i.css('.available::text').extract_first())
                if available_date != 'Available Now':
                    available_date = available_date.replace(',','')
                    available_date = available_date.split(' ')
                    available_date = available_date[-1]+'-'+available_date[1]+'-'+available_date[2]
                external_images_count = len(response.meta['photos'])
                if square_meters == 0:
                    square_meters = None
                else:
                    square_meters = sq_feet_to_meters(extract_number_only(square_meters))
                currency = "CAD"
                if 'furnish' in amenity:
                    furnished = True
                else:
                    furnished = False

                if 'elevator' in amenity:
                    elevator = True
                else:
                    elevator = False

                if 'balcon' in (description):
                    balcony = True
                else:
                    balcony = False

                if response.css('.parking-content p'):
                    parking = True
                else:
                    parking = False

                if 'laundry' in description:
                    washing_machine = True
                else:
                    washing_machine = False

                if 'dishwasher' in description:
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
                item_loader.add_value('bathroom_count', 1)
                item_loader.add_value('available_date', available_date)
                item_loader.add_value("images", response.meta['photos'])
                item_loader.add_value("external_images_count", external_images_count)
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", currency)
                item_loader.add_value("pets_allowed", response.meta['pets_allowed'])
                item_loader.add_value("furnished", furnished)
                item_loader.add_value("parking", parking)
                item_loader.add_value("elevator", elevator)
                item_loader.add_value("balcony", balcony)
                item_loader.add_value("washing_machine", washing_machine)
                item_loader.add_value("dishwasher", dishwasher)
                item_loader.add_value("landlord_name", response.meta['landlord_name'])
                item_loader.add_value("landlord_email", response.meta['landlord_email'])
                item_loader.add_value("landlord_phone", response.meta['landlord_phone'])

                yield item_loader.load_item()
