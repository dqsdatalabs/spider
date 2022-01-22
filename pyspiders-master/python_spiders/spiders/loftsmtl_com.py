import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json


class LoftsmtlComSpider(scrapy.Spider):
    name = 'loftsmtl_com'
    allowed_domains = ['loftsmtl.com']
    start_urls = ['https://api.theliftsystem.com/v2/search?locale=en&client_id=773&auth_token=sswpREkUtyeYjeoahA2i&city_id=1854&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=3100&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=1863%2C1854&pet_friendly=&offset=0&count=false']
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
        listings = response.css(
            '#suites > section > div > div.suite')

        hash = 1

        for listing in listings:
            item_loader = ListingLoader(response=response)
            rent = listing.css(
                'div.suite-rate.cell > span::text').get().split("$")[1]
            # baths = listing.css('p.suite-baths > span::text').get()
            # baths = math.ceil(float(baths))

            rooms = listing.css('div.suite-type.cell::text').get()
            if "1" in rooms:
                rooms = 1
            elif "2" in rooms or "2" in rooms:
                rooms = 2
            elif "3" in rooms:
                rooms = 3

            property_type = item['property_type']
            if "apartment" in property_type:
                property_type = "apartment"
            elif "house" in property_type:
                property_type = "house"

            space = None
            try:
                space_t = listing.css(
                    'div.suite-sqft.cell > span.value::text').get().strip()

                space = int(int(space_t)/10.7639)
            except:
                pass

            baths = listing.css(
                'div.suite-bath.cell > span.value::text').get().strip()

            images = response.css('div.cover::attr(style)').extract()

            for i in range(len(images)):
                images[i] = images[i].split("url('")[1].split("')")[0]

            feats = response.css('div.amenity-holder::text').extract()

            dishwasher = None
            laundry = None
            elevator = None
            try:
                for feat in feats:
                    if "Elevators" in feat:
                        elevator = True
                    elif "Dishwasher" in feat:
                        laundry = True
                    elif "laundry" in feat:
                        laundry = True
            except:
                pass

            parking = None
            if 'parking' in item['details']['overview']:
                parking = True

            item_loader.add_value('external_id', str(item['id']))
            item_loader.add_value('external_source', self.external_source)
            item_loader.add_value(
                'external_link', item['permalink'] + f"#{hash}")
            item_loader.add_value('title', item['name'])
            item_loader.add_value('description', item['details']['overview'])

            item_loader.add_value('property_type', property_type)
            item_loader.add_value('square_meters', int(int(space)*10.764))
            item_loader.add_value('room_count', baths)
            item_loader.add_value('bathroom_count', baths)

            item_loader.add_value('address', item['address']['address'])
            item_loader.add_value('city', item['address']['city'])
            item_loader.add_value('zipcode', item['address']['postal_code'])
            item_loader.add_value('dishwasher', dishwasher)
            item_loader.add_value('elevator', elevator)
            item_loader.add_value('washing_machine', laundry)
            item_loader.add_value('parking', parking)

            item_loader.add_value("latitude", item['geocode']['latitude'])
            item_loader.add_value("longitude", item['geocode']['longitude'])

            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count",
                                  len(images))

            # Monetary Status
            item_loader.add_value("rent", int(rent))
            item_loader.add_value("currency", "CAD")

            item_loader.add_value(
                "available_date", item['availability_status_label'])

            item_loader.add_value("landlord_email", item['client']['email'])
            item_loader.add_value("landlord_name", item['client']['name'])
            item_loader.add_value("landlord_phone", item['client']['phone'])

            hash += 1

            yield item_loader.load_item()
