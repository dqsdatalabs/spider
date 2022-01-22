import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math


class RentinottawaComSpider(scrapy.Spider):
    name = 'rentinottawa_com'
    allowed_domains = ['rentinottawa.com']
    start_urls = ['https://api.theliftsystem.com/v2/search?show_promotions=true&client_id=36&auth_token=sswpREkUtyeYjeoahA2i&city_id=2084&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=10000&only_available_suites=true&region=&keyword=false&property_types=apartments%2Chouses%2Csemi%2Crooms%2Ctownhouse%2Csingle-family-home&amenities=&order=min_rate+ASC%2C+max_rate+ASC%2C+min_bed+ASC%2C+max_bed+ASC&limit=60&offset=0&count=false']
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
        item_loader = ListingLoader(response=response)

        images = response.css("div.image > img::attr(src)").extract()

        property_type = item['property_type']
        if "apartment" in property_type or "plex" in property_type or "Semi" in property_type:
            property_type = "apartment"
        elif "house" in property_type or "home" in property_type:
            property_type = "house"
        elif "Rooms" in property_type:
            property_type = "room"

        space = None
        rooms = int(float(item['statistics']['suites']['bedrooms']['max']))
        bathrooms = math.ceil(
            float(item['statistics']['suites']['bedrooms']['max']))
        try:
            space_t = float(item['statistics']['suites']['square_feet']['max'])
            space = int(int(space_t)/10.7639)
        except:
            pass

        if space == 0:
            space = None
        if rooms == 0:
            rooms = None
        if bathrooms == 0:
            bathrooms = None

        feats = response.css(
            '#content > section.bottom-details > div > div:nth-child(1) > div > div.cms-content > ul  > li::text').extract()

        washer = None
        laundry = None
        for feat in feats:
            if "Dishwasher" in feat:
                washer = True
            if "Washer" in feat:
                laundry = True

        item_loader.add_value('external_id', str(item['id']))
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('external_link', item['permalink'])
        item_loader.add_value('title', item['name'])
        item_loader.add_value('description', item['details']['overview'])

        item_loader.add_value('property_type', property_type)
        item_loader.add_value('square_meters', int(int(space)*10.764))
        item_loader.add_value('room_count', rooms)
        item_loader.add_value('bathroom_count', bathrooms)

        item_loader.add_value('address', item['address']['address'])
        item_loader.add_value('city', item['address']['city'])
        item_loader.add_value('zipcode', item['address']['postal_code'])
        # item_loader.add_value('balcony', Balconies)
        # item_loader.add_value('elevator', elevator)
        item_loader.add_value('washing_machine', laundry)
        item_loader.add_value('dishwasher', washer)

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
