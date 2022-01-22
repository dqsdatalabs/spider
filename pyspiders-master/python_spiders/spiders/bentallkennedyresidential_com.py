# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math


class BentallkennedyresidentialComSpider(scrapy.Spider):
    name = 'bentallkennedyresidential_com'
    allowed_domains = ['bentallkennedyresidential.com']
    start_urls = ['https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=3227&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=3400&max_rate=3500&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false', "https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=3201&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1500&max_rate=4200&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false", "https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=408&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=600&max_rate=1800&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false", "https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=845&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1100&max_rate=1400&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false", "https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=1969&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1800&max_rate=1800&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false", "https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=1517&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=2000&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false", "https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=3377&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1300&max_rate=5000&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false", "https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=3284&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1400&max_rate=1900&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false", "https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=2860&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1100&max_rate=1400&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false", "https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=387&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1600&max_rate=2200&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false", "https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=1174&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1500&max_rate=2000&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false", "https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=1425&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1400&max_rate=2000&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false", "https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=2042&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1600&max_rate=1700&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false", "https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=2015&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1600&max_rate=2300&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false",
                  "https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=3133&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1700&max_rate=7500&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false",
                  "https://api.theliftsystem.com/v2/search?client_id=97&auth_token=sswpREkUtyeYjeoahA2i&city_id=1863&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=900&max_rate=3000&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false", ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url,
                          callback=self.parse,
                          body='',
                          method='GET')

    def parse(self, response):
        parsed_response = json.loads(response.body)

        for item in parsed_response:
            item_loader = ListingLoader(response=response)

            property_type = item['property_type']
            if "apartment" in property_type:
                property_type = "apartment"
            elif "house" in property_type:
                property_type = "house"

            space = None
            try:
                space = float(item['statistics']['suites']
                              ['square_feet']['max'])
                space = int(int(space)/10.7639)
            except:
                pass

            item_loader.add_value('external_id', str(item['id']))
            item_loader.add_value('external_source', self.external_source)
            item_loader.add_value('external_link', item['permalink'])
            item_loader.add_value('title', item['name'])
            item_loader.add_value('description', item['details']['overview'])

            item_loader.add_value('property_type', property_type)
            item_loader.add_value('square_meters', int(int(space)*10.764))
            item_loader.add_value('room_count', int(
                item['statistics']['suites']['bedrooms']['max']))
            item_loader.add_value('bathroom_count', int(
                item['statistics']['suites']['bathrooms']['max']))

            item_loader.add_value('address', item['address']['address'])
            item_loader.add_value('city', item['address']['city'])
            item_loader.add_value('zipcode', item['address']['postal_code'])

            item_loader.add_value("latitude", item['geocode']['latitude'])
            item_loader.add_value("longitude", item['geocode']['longitude'])

            item_loader.add_value("images", [item['photo_path']])
            item_loader.add_value("external_images_count",
                                  len([item['photo_path']]))

            # Monetary Status
            item_loader.add_value("rent", int(
                item['statistics']['suites']['rates']['max']))
            item_loader.add_value("currency", "CAD")

            item_loader.add_value(
                "available_date", item['availability_status_label'])

            item_loader.add_value("landlord_phone", item['client']['phone'])
            item_loader.add_value("landlord_email", item['client']['email'])
            item_loader.add_value("landlord_name", item['client']['name'])

            yield item_loader.load_item()
