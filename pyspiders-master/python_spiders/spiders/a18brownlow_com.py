import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json


class A18brownlowComSpider(scrapy.Spider):
    name = '18brownlow_com'
    allowed_domains = ['18brownlow.com']
    start_urls = [
        'https://18brownlow.com/api/suites/read?apiKey=b2951d2b72624fd4c204&showSuiteTypes=true&showSuites=false']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url,
                          callback=self.parse,
                          body='',
                          method='GET')

    def parse(self, response):
        parsed_response = json.loads(response.body)

        for item in parsed_response['suites']:
            link = f'https://18brownlow.com/suites/{item["id"]}'
            yield Request(link,
                          callback=self.populate_item,
                          meta={"item": item}
                          )

    def populate_item(self, response):
        item = response.meta["item"]
        item_loader = ListingLoader(response=response)

        space = None
        try:
            space_t = item['size']

            space = int(int(space_t)/10.7639)
        except:
            pass

        images = response.css(
            '#tns1 > div.slide.tns-item.tns-slide-cloned.tns-slide-active > img::attr(src)').extract()

        floor_plan = 'https://18brownlow.com/' + item['floorplan']

        item_loader.add_value('external_id', str(item['id']))
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('external_link', response.url)
        item_loader.add_value('title', item['name'])

        item_loader.add_value('property_type', 'room')
        item_loader.add_value('square_meters', int(int(space)*10.764))
        item_loader.add_value('room_count', int(item['numBedrooms']))
        item_loader.add_value('bathroom_count', int(item['numBathrooms']))

        item_loader.add_value("images", images)
        item_loader.add_value("floor_plan_images", floor_plan)
        item_loader.add_value("external_images_count",
                              len(images))

        # Monetary Status
        item_loader.add_value("rent", int(item['rent']))
        item_loader.add_value("currency", "CAD")

        item_loader.add_value(
            "available_date", item['availableOn'].split('T')[0])

        item_loader.add_value("landlord_name", '18 Brownlow')
        item_loader.add_value("landlord_email", 'hello@18brownlow.com')
        item_loader.add_value("landlord_phone", '416-485-4342')

        item_loader.add_value("position", self.position)
        self.position += 1

        yield item_loader.load_item()
