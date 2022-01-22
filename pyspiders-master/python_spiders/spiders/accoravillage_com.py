# -*- coding: utf-8 -*-
# Author: Mahmoud Wessam
import scrapy
from ..loaders import ListingLoader


class AccoravillageComSpider(scrapy.Spider):
    name = "accoravillage_com"
    start_urls = ['https://accoravillage.com/find-your-home/']
    allowed_domains = ["accoravillage.com"]
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        for appartment in response.css("#accora-unit-listing > div > div > div > div"):
            url = appartment.css(
                "div.jet-engine-listing-overlay-wrap").attrib['data-url']
            yield scrapy.Request(url,
                                 callback=self.populate_item,
                                 )

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        external_id = response.xpath("//link[@rel='shortlink']/@href").get()

        title = response.css(
            'h1.elementor-heading-title.elementor-size-default::text').get()

        rent = response.css('div.jet-listing-dynamic-field__content::text').get().split(
            '/')[0].split('$')[1].replace(",", "")

        images = response.css('img.sp-thumbnail::attr(src)').extract()

        counter = 0

        while counter < len(images):
            if "svg" in images[counter]:
                images.pop(counter)
            counter += 1

        description = response.css(
            'div.elementor-widget-container>p::text').get()

        square_meters = response.css(
            'div.jet-listing-dynamic-field__content > ul > li::text').get().strip().split(" ")[0]

        ameneties = response.css(
            'div.jet-listing-dynamic-repeater__items > ul > li > div::text').extract()

        room_count = None
        if "studio" in response.url:
            room_count = 1
        else:
            room_count = response.url.split('-bedroom')[0][-1]

        if isinstance(room_count, str):
            room_count = 1

        dishwasher = None
        balcony = None
        washing_machine = None
        parking = None
        swimming_pool = None
        pets_allowed = None
        for item in ameneties:
            if "dishwashers" in item:
                dishwasher = True
            if "balconies" in item:
                balcony = True
            if "laundry" in item:
                washing_machine = True
            if "parking" in item:
                parking = True
            if "Pet friendly" in item:
                pets_allowed = True
            if "pool" in item:
                swimming_pool = True

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value(
            "external_source", self.external_source)  # String

        item_loader.add_value("external_id",  "{}".format(
            external_id.split("=")[-1].strip()))   # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", 'Ottawa')  # String
        # item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", 'Ottawa')  # String
        # item_loader.add_value("latitude", latitude) # String
        # item_loader.add_value("longitude", longitude) # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", 'apartment')
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        # item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "CAD")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'ACCORA VILLAGE')  # String
        item_loader.add_value("landlord_phone", '613-366-5019')  # String
        item_loader.add_value(
            "landlord_email", 'connect@accoravillage.com')  # String

        self.position += 1
        yield item_loader.load_item()
