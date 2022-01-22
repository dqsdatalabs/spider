import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math


class CsmanagementCaSpider(scrapy.Spider):
    name = "csmanagement_ca"
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'
    handle_httpstatus_list = [500, 400]
    custom_settings = {
        #   "PROXY_ON": True,
        "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307],
        "HTTPCACHE_ENABLED": False
    }

    def start_requests(self):
        yield Request(url='https://api.theliftsystem.com/v2/search?client_id=386&auth_token=sswpREkUtyeYjeoahA2i&city_id=845&geocode=&min_bed=0&max_bed=10&min_bath=0&max_bath=10&min_rate=500&max_rate=2600&min_sqft=0&max_sqft=10000&only_available_suites=true&region=&keyword=false&property_types=low-rise-apartment%2Chouse%2Cmid-rise-apartment%2Chigh-rise-apartment%2Cluxury-apartment%2Cmulti-unit-house%2Csingle-family-home%2Ctownhouse%2Csemi%2Cduplex%2Cindustrial%2Cland%2Ctriplex%2Coffice%2Cfourplex%2Cretail%2Cwarehouse%2Cmobile-home%2Cagricultural%2Ccampground%2Cconstruction%2Chotel%2Csubdivision%2Cmarina%2Cmotel%2Cstorage_facility%2Crooms&order=min_rate+ASC%2C+max_rate+ASC%2C+min_bed+ASC%2C+max_bed+ASC&limit=50&neighbourhood=&amenities=&promotions=&pet_friendly=&offset=0&count=false',
                      callback=self.parse,
                      body='',
                      method='GET')

    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            yield Request(item["permalink"], callback=self.populate_item, meta={"item": item})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item = response.meta['item']

        pets = item['pet_friendly']
        if type(pets) is not bool:
            pets = None

        property_type = item['property_type']
        if "apartment" in property_type or "plex" in property_type or "condo" in property_type:
            property_type = "apartment"
        elif "house" in property_type or "home" in property_type:
            property_type = "house"

        space = float(item['statistics']['suites']['square_feet']['max'])
        space = int(space)/10.7639

        feats = response.css(
            '#content > div.container.display-none > div > div.mainbar.span8 > div.tab-content-container > section > div > div > section.widget.amenities > ul > li')

        washer = None
        Dishwasher = None
        parking = None
        for itemaya in feats:
            try:
                if "Washer in suite" in itemaya.css("::text").get():
                    washer = True
                elif "parking" in itemaya.css("::text").get():
                    parking = True
            except:
                pass

        description = item['details']['overview'].replace(
            '<p>', "").replace('</p>', '')
        if "dishwasher" in description:
            Dishwasher = True

        available_date = None
        if response.css('#content > div.container.display-none > div > div.sidebar.span4 > section > section > div > div:nth-child(1) > div.inquire-availability-container > div.suite-availability > div.available-now'):
            available_date = 'Available Now'

        images = response.css(
            '#slickslider-default-id-0 > div > div::attr(data-src2x)').extract()
        

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id",  str(item['id']))
        item_loader.add_value("title", item['name'])
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", int(int(int(space))*10.764))
        item_loader.add_value(
            "room_count", math.ceil(float(item['matched_beds'][-1])))
        # item_loader.add_value("floor", floor)
        item_loader.add_value(
            "bathroom_count", math.ceil(float(item['matched_baths'][-1])))
        item_loader.add_value("address", item['name'])
        item_loader.add_value("city", item['address']['city'])
        item_loader.add_value("zipcode", item['address']['postal_code'])
        item_loader.add_value("available_date", available_date)

        item_loader.add_value("pets_allowed", pets)
        item_loader.add_value("parking", parking)
        item_loader.add_value("dishwasher", Dishwasher)
        item_loader.add_value("parking", item['parking']['indoor'])
        item_loader.add_value("washing_machine", washer)

        item_loader.add_value("latitude", str(item['geocode']['latitude']))
        item_loader.add_value("longitude", str(item['geocode']['longitude']))

        # Images
        # item_loader.add_value("images", item["photo_path"])
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", images)

        # # Monetary Status
        item_loader.add_value(
            "rent", int(item['statistics']['suites']['rates']['max']))
        item_loader.add_value("currency", "CAD")

        # LandLord Details
        item_loader.add_value("landlord_phone", item['client']['phone'])
        item_loader.add_value("landlord_email", item['client']['email'])
        item_loader.add_value("landlord_name", item['client']['name'])

        yield item_loader.load_item()
