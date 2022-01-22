import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math


class KeypropComSpider(scrapy.Spider):
    name = 'keyprop_com'
    allowed_domains = ['keyprop.com']
    start_urls = ['https://api.theliftsystem.com/v2/search?locale=en&client_id=157&auth_token=sswpREkUtyeYjeoahA2i&city_id=1410&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=1900&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false']
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
            yield Request(url=item['permalink'],
                          callback=self.populate_item,
                          meta={"item": item}
                          )

    def populate_item(self, response):
        item = response.meta["item"]
        # listings = response.css('div.suites > div.suite')

        # for listing in listings:
        item_loader = ListingLoader(response=response)
        # rent = listing.css('div.rate-value::text').get().split("$")[1]
        # baths = listing.css('p.suite-baths > span::text').get()
        # baths = math.ceil(float(baths))

        # rooms = listing.css('a.suite-photo::text').get()
        # if "One" in rooms:
        #     rooms = 1
        # elif "Two" in rooms or "2" in rooms:
        #     rooms = 2
        # elif "Three" in rooms:
        #     rooms = 3

        property_type = item['property_type']
        if "apartment" in property_type:
            property_type = "apartment"
        elif "house" in property_type:
            property_type = "house"

        space = float(item['statistics']['suites']['square_feet']['max'])
        space = int(space)/10.7639

        feats = response.css(
            '#amenities > div > div > div > div.amenity-group > div::text').extract()

        Balconies = None
        parking = None
        elevator = None
        try:
            for feat in feats:
                if "Elevators" in feat:
                    elevator = True
                elif "parking" in feat:
                    parking = True
                elif "Balconies" in feat:
                    Balconies = True
        except:
            pass

        images = response.css('div.cover::attr(style)').extract()

        for i in range(len(images)):
            images[i] = images[i].split(":url('")[1].split("')")[0]

        item_loader.add_value('external_id', str(item['id']))
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('external_link', item['permalink'])
        item_loader.add_value('title', item['name'])
        item_loader.add_value('description', item['details']['overview'])

        item_loader.add_value('property_type', property_type)
        item_loader.add_value('square_meters', int(int(int(space))*10.764))
        item_loader.add_value(
            'room_count', item['statistics']['suites']['bedrooms']['max'])
        item_loader.add_value(
            "bathroom_count", math.ceil(
                float(item['statistics']['suites']['bedrooms']['max'])))

        item_loader.add_value('address', item['address']['address'])
        item_loader.add_value('city', item['address']['city'])
        item_loader.add_value('zipcode', item['address']['postal_code'])
        item_loader.add_value('balcony', Balconies)
        item_loader.add_value('elevator', elevator)
        item_loader.add_value('parking', parking)

        item_loader.add_value("latitude", item['geocode']['latitude'])
        item_loader.add_value("longitude", item['geocode']['longitude'])

        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count",
                              len(images))

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
