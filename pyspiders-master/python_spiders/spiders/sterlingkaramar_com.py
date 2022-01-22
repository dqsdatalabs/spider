import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json


class SterlingkaramarComSpider(scrapy.Spider):
    name = 'sterlingkaramar_com'
    allowed_domains = ['sterlingkaramar.com']
    start_urls = ['https://website-gateway.rentsync.com/v1/sterlingkaramar/search?propertyFilters=categoryTypes:residential&unitFilters=availability:available&order=unit:available~DESC%7Cunit:rateMin~ASC&include=photos,buildingTypes&groupUnitSummaryBy=availability&limit=8&addSelect=email,phone&showAllProperties=true']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    pos = 1

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url,
                          callback=self.parse,
                          body='',
                          method='GET')

    def parse(self, response):
        parsed_response = json.loads(response.body)

        for item in parsed_response['data']:
            url = "https://www.sterlingkaramar.com/apartments-for-rent/" + \
                item['permaLink']
            yield Request(url=url,
                          callback=self.populate_item,
                          meta={"item": item}
                          )

    def populate_item(self, response):
        item = response.meta["item"]
        listings = response.css('#propertyDetails > div.property-details__content-wrap.uk3-text-left.uk3-container.sidebar-content.sidebar-content--reverse > div.property-details__content.sidebar-content__content.sidebar-content__content--transparent.sidebar-content__content--full.uk3-padding-remove-right > div.property-details__building-highlights.property-details__box.uk3-section.uk3-padding-remove-bottom > div.property-details__box-content.property-details__box-content--units.alt-list > div.property-details__box-suite-table > div > table > tbody > tr')

        description = response.css('#propertyDetails > div.property-details__content-wrap.uk3-text-left.uk3-container.sidebar-content.sidebar-content--reverse > div.property-details__content.sidebar-content__content.sidebar-content__content--transparent.sidebar-content__content--full.uk3-padding-remove-right > div.property-details__about-building.property-details__box.uk3-section.uk3-padding-remove-bottom > div.page-content.property-details__box-content.alt-list > p::text').get()
        images = response.css('img::attr(data-src)').extract()

        address = response.css(
            '#propertyHero > div > div.property-hero__content.faded.faded--alt.light.uk3-text-left > div > div > p::text').get()
        zipcode = address.split(',')[-1].strip()
        city = "Ontario"

        hash = 1
        for listing in listings:
            item_loader = ListingLoader(response=response)
            rent = listing.css(
                'td.suite-table__td.suite-table__td--min-rate > span:nth-child(2)::text').get().split("$")[1]
            if rent is None:
                pass

            rooms = listing.css(
                'td.suite-table__td.suite-table__td--type > span:nth-child(2)::text').get()
            if "1" in rooms:
                rooms = 1
            elif "2" in rooms or "2" in rooms:
                rooms = 2
            elif "3" in rooms:
                rooms = 3
            else:
                rooms = 1

            property_type = item['buildingTypeId']
            if "apartment" in property_type or "condo" in property_type.lower() or "plex" in property_type:
                property_type = "apartment"
            elif "house" in property_type or "home" in property_type:
                property_type = "house"

            feats = response.css('ul.uk3-list > li::text').extract()

            Balconies = None
            parking = None
            laundry = None
            dishwasher = None

            for feat in feats:
                if "Balconies" in feat:
                    Balconies = True
                elif "Laundry" in feat:
                    laundry = True
                elif "Parking" in feat:
                    parking = True
                elif "dishwasher" in feat:
                    dishwasher = True

            if response.css('div.alt-list.alt-list--parking'):
                parking = True

            item_loader.add_value('external_id', str(item['id']))
            item_loader.add_value('external_source', self.external_source)
            item_loader.add_value('external_link', response.url + f'#{hash}')
            item_loader.add_value('title', item['permaLink'])
            item_loader.add_value('description', description)

            item_loader.add_value('property_type', property_type)
            item_loader.add_value('room_count', rooms)

            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('parking', parking)
            item_loader.add_value('balcony', Balconies)
            item_loader.add_value('washing_machine', laundry)
            item_loader.add_value('dishwasher', dishwasher)

            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count",
                                  len(images))

            # Monetary Status
            item_loader.add_value("rent", int(rent))
            item_loader.add_value("currency", "CAD")

            item_loader.add_value("landlord_name", 'Sterling Karamar')
            item_loader.add_value("landlord_phone", item['phone'])
            item_loader.add_value("landlord_email", item['email'])

            item_loader.add_value("position", self.pos)

            hash += 1
            self.pos += 1

            yield item_loader.load_item()
