import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math


class ParkpropertyCaSpider(scrapy.Spider):
    name = 'parkproperty_ca'
    allowed_domains = ['parkproperty.ca']
    start_urls = ['https://api.theliftsystem.com/v2/search?client_id=65&auth_token=sswpREkUtyeYjeoahA2i&city_id=3284&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1300&max_rate=1900&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
                  "https://api.theliftsystem.com/v2/search?client_id=65&auth_token=sswpREkUtyeYjeoahA2i&city_id=3117&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=900&max_rate=1700&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false",
                  "https://api.theliftsystem.com/v2/search?client_id=65&auth_token=sswpREkUtyeYjeoahA2i&city_id=1818&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1600&max_rate=2100&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false",
                  "https://api.theliftsystem.com/v2/search?client_id=65&auth_token=sswpREkUtyeYjeoahA2i&city_id=3017&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1200&max_rate=1700&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false",
                  "https://api.theliftsystem.com/v2/search?client_id=65&auth_token=sswpREkUtyeYjeoahA2i&city_id=2566&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1300&max_rate=3800&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false",
                  "https://api.theliftsystem.com/v2/search?client_id=65&auth_token=sswpREkUtyeYjeoahA2i&city_id=415&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1200&max_rate=2500&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false",
                  "https://api.theliftsystem.com/v2/search?client_id=65&auth_token=sswpREkUtyeYjeoahA2i&city_id=1425&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1300&max_rate=1500&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false",
                  "https://api.theliftsystem.com/v2/search?client_id=65&auth_token=sswpREkUtyeYjeoahA2i&city_id=3133&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1000&max_rate=3500&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false",
                  "https://api.theliftsystem.com/v2/search?client_id=65&auth_token=sswpREkUtyeYjeoahA2i&city_id=1837&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1200&max_rate=2300&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false",
                  "https://api.theliftsystem.com/v2/search?client_id=65&auth_token=sswpREkUtyeYjeoahA2i&city_id=1154&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1600&max_rate=1800&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false",
                  "https://api.theliftsystem.com/v2/search?client_id=65&auth_token=sswpREkUtyeYjeoahA2i&city_id=902&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1300&max_rate=2500&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false",
                  "https://api.theliftsystem.com/v2/search?client_id=65&auth_token=sswpREkUtyeYjeoahA2i&city_id=2042&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1200&max_rate=2300&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false",
                  "https://api.theliftsystem.com/v2/search?client_id=65&auth_token=sswpREkUtyeYjeoahA2i&city_id=387&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1600&max_rate=2200&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false"]
    country = 'canada'
    locale = 'en_ca'
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
            yield Request(url=item['permalink'],
                          callback=self.populate_item,
                          meta={"item": item}
                          )

    def populate_item(self, response):
        item = response.meta["item"]
        listings = response.css('#availabilitycontent > div > div')

        for listing in listings:
            item_loader = ListingLoader(response=response)

            images = response.css('li.gallery-image > a::attr(href)').extract()

            rent = listing.css('p.suite-rent > span::text').get().split("$")[1]
            baths = listing.css('p.suite-baths > span::text').get()
            baths = math.ceil(float(baths))

            rooms = listing.css('h3.suite-type::text').get()
            if "Bachelor" in rooms:
                property_type = "studio"
                rooms = 1
            else:
                if "One" in rooms:
                    rooms = 1
                elif "Two" in rooms or "2" in rooms:
                    rooms = 2
                elif "Three" in rooms:
                    rooms = 3

                property_type = item['property_type']
                if "apartment" in property_type:
                    property_type = "apartment"
                elif "house" in property_type:
                    property_type = "house"

            space = None
            try:
                space = listing.css('p.suite-sqft > span::text').get()
                space = int(int(space)/10.7639)
            except:
                pass

            feats = response.css('li.amenity')

            pets = None
            parking = None
            available_date = None
            try:
                for feat in feats:
                    if "Pet" in feat.css("li::text").get():
                        pets = True
                    elif "Parking" in feat.css("li::text").get():
                        parking = True
            except:
                pass

            available_date = listing.css('p.suite-availability::text').get()

            item_loader.add_value('external_id', str(item['id']))
            item_loader.add_value('external_source', self.external_source)
            item_loader.add_value('external_link', item['permalink'])
            item_loader.add_value('title', item['name'])
            item_loader.add_value('description', item['details']['overview'])

            item_loader.add_value('property_type', property_type)
            item_loader.add_value('square_meters', int(int(space)*10.764))
            item_loader.add_value('room_count', rooms)
            item_loader.add_value('bathroom_count', baths)

            item_loader.add_value('address', item['address']['address'])
            item_loader.add_value('city', item['address']['city'])
            item_loader.add_value('zipcode', item['address']['postal_code'])
            item_loader.add_value('parking', parking)
            item_loader.add_value('pets_allowed', pets)

            item_loader.add_value("latitude", item['geocode']['latitude'])
            item_loader.add_value("longitude", item['geocode']['longitude'])

            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

            # # Monetary Status
            item_loader.add_value("rent", int(rent))
            item_loader.add_value("currency", "CAD")

            item_loader.add_value("available_date", available_date)

            item_loader.add_value("landlord_phone", item['client']['phone'])
            item_loader.add_value("landlord_email", item['client']['email'])
            item_loader.add_value("landlord_name", item['client']['name'])

            yield item_loader.load_item()
