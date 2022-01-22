# Author: Mohamed Helmy
import scrapy
from ..loaders import ListingLoader
from scrapy import Request
import json 

class AshManagementGroupSpider(scrapy.Spider):
    name = "ashmanagementgroup"
    allowed_domains = ["ashmanagementgroup.com"]
    start_urls = [
        'https://api.theliftsystem.com/v2/search?locale=en&client_id=637&auth_token=sswpREkUtyeYjeoahA2i&city_id=207&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=1600&min_sqft=0&max_sqft=100000&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=3377%2C331%2C788%2C3100%2C33013%2C2587%2C472%2C207&pet_friendly=&offset=0&count=false',
        'https://api.theliftsystem.com/v2/search?locale=en&client_id=637&auth_token=sswpREkUtyeYjeoahA2i&city_id=2587&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=1600&min_sqft=0&max_sqft=100000&show_all_properties=true&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
        'https://api.theliftsystem.com/v2/search?locale=en&client_id=637&auth_token=sswpREkUtyeYjeoahA2i&city_id=3100&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=1600&min_sqft=0&max_sqft=100000&show_all_properties=true&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
        'https://api.theliftsystem.com/v2/search?locale=en&client_id=637&auth_token=sswpREkUtyeYjeoahA2i&city_id=331&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=1600&min_sqft=0&max_sqft=100000&show_all_properties=true&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
        'https://api.theliftsystem.com/v2/search?locale=en&client_id=637&auth_token=sswpREkUtyeYjeoahA2i&city_id=207&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=1600&min_sqft=0&max_sqft=100000&show_all_properties=true&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
        'https://api.theliftsystem.com/v2/search?locale=en&client_id=637&auth_token=sswpREkUtyeYjeoahA2i&city_id=472&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=1600&min_sqft=0&max_sqft=100000&show_all_properties=true&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
        'https://api.theliftsystem.com/v2/search?locale=en&client_id=637&auth_token=sswpREkUtyeYjeoahA2i&city_id=788&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=1600&min_sqft=0&max_sqft=100000&show_all_properties=true&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
        'https://api.theliftsystem.com/v2/search?locale=en&client_id=637&auth_token=sswpREkUtyeYjeoahA2i&city_id=33013&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=1600&min_sqft=0&max_sqft=100000&show_all_properties=true&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false'
    ]
    country = 'canada'
    locale = 'en'
    execution_type = 'development'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    
    def start_requests(self):
        for url in self.start_urls:
            yield Request(url = url,
                          callback=self.parse,
                          body='',
                          method='GET')

    def parse(self, response):
        parsed_data = json.loads(response.body)
        
        for item in parsed_data:
            item_loader = ListingLoader(response=response)
                              
            property_type = item['property_type']
            room_count = item['statistics']['suites']['bedrooms']['max']
            area = item['statistics']['suites']['square_feet']['max']
            bathroom_count = item['statistics']['suites']['bathrooms']['max']
            rent = item['statistics']['suites']['rates']['max']
            
            
            if (rent is None or rent == int(0)):
                continue
            if (bathroom_count is None):
                bathroom_count = int(0)
            if (area is None):
                area = int(0)
            if (room_count is None):
                room_count = int(0)
            if "apartment" in property_type:
                property_type = "apartment"
            if "house" in property_type:
                property_type = "house"
            
            item_loader.add_value('external_id', str(item['id']))
            item_loader.add_value('external_source', self.external_source)
            item_loader.add_value('external_link', item['permalink'])
            item_loader.add_value('title', item['name'])
            item_loader.add_value('description', item['details']['overview'])
            
            item_loader.add_value('property_type', property_type)
            
            item_loader.add_value('square_meters', int(int(area)*10.764))
            item_loader.add_value('room_count', room_count)
            item_loader.add_value('bathroom_count', bathroom_count)

            item_loader.add_value('address', item['address']['address'])
            item_loader.add_value('city', item['address']['city'])
            item_loader.add_value('zipcode', item['address']['postal_code'])

            item_loader.add_value("latitude", item['geocode']['latitude'])
            item_loader.add_value("longitude", item['geocode']['longitude'])

            item_loader.add_value("images", [item['photo_path']])
            item_loader.add_value("external_images_count",
                                  len([item['photo_path']]))
            
            item_loader.add_value("pets_allowed", item['pet_friendly'])
            # Rent and Availability
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "CAD")

            item_loader.add_value(
                "available_date", item['availability_status_label'])

            # Contact Information
            item_loader.add_value("landlord_phone", item['client']['phone'])
            item_loader.add_value("landlord_email", item['client']['email'])
            item_loader.add_value("landlord_name", item['client']['name'])
            
            
            
            yield item_loader.load_item()
