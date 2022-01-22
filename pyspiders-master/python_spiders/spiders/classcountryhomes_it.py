import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..user_agents import random_user_agent


class ClasscountryhomesSpider(scrapy.Spider):
    name = "classcountryhomes_it"
    execution_type = 'development'
    country = 'italy'
    external_source = "classcountryhomes"
    locale = 'it'
    handle_httpstatus_list = [500, 400]
    custom_settings = {
        #   "PROXY_ON": True,
        "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307],
        "HTTPCACHE_ENABLED": False
    }

    def start_requests(self):
        yield Request(url='https://www.classcountryhomes.it/en/wp-json/myhome/v1/estates?currency=any&mh_lang=it',
                      callback=self.parse,
                      body='',
                      method='POST')

    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response['results']:
            yield Request(item["link"], callback=self.populate_item, meta={"item": item})

    # 2. SCRAPING level 2

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rent = response.meta["item"]["price"][0]["price"].split("€")[0]

        if "." in rent:
            rent_array = rent.split(".")
            rent = rent_array[0] + rent_array[1]

        description = response.css(
            "div.mh-estate__section.mh-estate__section--description > p>span::text").get()

        sale_or_rent = response.css(
            "#mh-estate_attribute--1 > a::text").get().strip()

        if sale_or_rent == "Vendita":
            return

        property_type = response.css(
            "#mh-estate_attribute--2 > a::text").get().strip()

        if property_type == "Appartamenti":
            property_type = "apartment"
        elif property_type == "Ville":
            property_type = "house"

        features = response.meta["item"]["attributes"]
        square_meters = None
        rooms = None
        floor = None
        bathrooms = None
        for item in features:
            if "Superficie commerciale" in item["name"]:
                square_meters = item["values"][0]["value"]
            if "Camere da letto" in item["name"]:
                rooms = item["values"][0]["value"]
            if "Livelli" in item["name"]:
                floor = item["values"][0]["value"]
            if "Bagni" in item["name"]:
                bathrooms = item["values"][0]["value"]

        images = []

        for image in response.meta["item"]["gallery"]:
            images.append(image['image'])

        # MetaData
        item_loader.add_value("external_link", response.meta["item"]["link"])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", response.meta["item"]["name"])
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", int(square_meters))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("floor", floor)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", response.meta["item"]["address"])

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "0668807642")
        item_loader.add_value("landlord_email", "barbara@cchroma.it")
        item_loader.add_value("landlord_name", "Class Country Homes")

        yield item_loader.load_item()
