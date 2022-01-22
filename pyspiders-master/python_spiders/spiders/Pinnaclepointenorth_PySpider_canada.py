# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import sq_feet_to_meters


class PinnaclepointenorthPyspiderCanadaSpider(scrapy.Spider):
    name = "Pinnaclepointenorth_PySpider_canada"
    start_urls = ['https://pinnaclepointenorth.com/suites-page/']
    allowed_domains = ["pinnaclepointenorth.com"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.populate_item)

    # 2. SCRAPING level 2
    # def parse(self, response, **kwargs):
    #     for url in self.start_urls:
    #         yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        apartments_divs = response.xpath('.//div[contains(@class, "et_pb_column_1_4")]')
        square_meters = None
        bathroom_count = None
        for apartment_div in apartments_divs:
            item_loader = ListingLoader(response=response)
            floor_plan_images = apartment_div.xpath('.//div[1]//a/@href').extract()
            apartment_info = apartment_div.xpath('.//div[3]')
            room_count = apartment_info.xpath('.//div/text()').extract()
            if room_count:
                room_count = room_count[0].replace(" Bedroom", "")
                room_count = room_count[0].replace("+ Den", "")
                room_count = int(room_count.strip())
            rent = apartment_info.xpath('.//div//h4/text()').extract()
            if rent:
                rent = rent[0].replace("/mo", "")
                rent = rent.replace("$", "")
                rent = rent.replace(",", "")
                rent = int(rent)
            details = apartment_info.xpath('.//div//p/text()').extract()
            if details:
                details = details[0].split("•")

                square_meters_all = details[0]
                square_meters = (square_meters_all.split(" Sq"))[0]
                square_meters = square_meters.replace(",", "")
                square_meters = sq_feet_to_meters(square_meters)

                bathroom_count = [int(s) for s in details[2].split() if s.isdigit()]
                bathroom_count = bathroom_count[0]

            landlord_number = response.xpath('.//div[contains(@class, "et_pb_text_22")]//div[contains(@class, "et_pb_text_inner")]//p//span//a/text()')[0].extract()
            landlord_email = response.xpath('.//div[contains(@class, "et_pb_text_22")]//div[contains(@class, "et_pb_text_inner")]//p//span//a//span/text()')[0].extract()
            landlord_name = "Pinnacle Pointe North"

            # # MetaData
            if rent:
                item_loader.add_value("external_link", response.url) # String
                item_loader.add_value("external_source", self.external_source) # String

                #item_loader.add_value("external_id", external_id) # String
                item_loader.add_value("position", self.position) # Int
                #item_loader.add_value("title", title) # String
                #item_loader.add_value("description", description) # String

                # # Property Details
                #item_loader.add_value("city", city) # String
                #item_loader.add_value("zipcode", zipcode) # String
                #item_loader.add_value("address", address) # String
                #item_loader.add_value("latitude", latitude) # String
                #item_loader.add_value("longitude", longitude) # String
                #item_loader.add_value("floor", floor) # String
                #item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
                item_loader.add_value("square_meters", square_meters) # Int
                item_loader.add_value("room_count", room_count) # Int
                item_loader.add_value("bathroom_count", bathroom_count) # Int

                #item_loader.add_value("available_date", available_date) # String => date_format

                #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                #item_loader.add_value("furnished", furnished) # Boolean
                #item_loader.add_value("parking", parking) # Boolean
                #item_loader.add_value("elevator", elevator) # Boolean
                #item_loader.add_value("balcony", balcony) # Boolean
                #item_loader.add_value("terrace", terrace) # Boolean
                #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                #item_loader.add_value("washing_machine", washing_machine) # Boolean
                #item_loader.add_value("dishwasher", dishwasher) # Boolean

                # # Images
                #item_loader.add_value("images", images) # Array
                #item_loader.add_value("external_images_count", len(images)) # Int
                item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                # # Monetary Status
                item_loader.add_value("rent", rent) # Int
                #item_loader.add_value("deposit", deposit) # Int
                #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                #item_loader.add_value("utilities", utilities) # Int
                item_loader.add_value("currency", "CAD") # String

                #item_loader.add_value("water_cost", water_cost) # Int
                #item_loader.add_value("heating_cost", heating_cost) # Int

                #item_loader.add_value("energy_label", energy_label) # String

                # # LandLord Details
                item_loader.add_value("landlord_name", landlord_name) # String
                item_loader.add_value("landlord_phone", landlord_number) # String
                item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()
