# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_location_from_address


class RoyalrealtyPyspiderCanadaSpider(scrapy.Spider):
    name = "RoyalRealty"
    start_urls = ['https://royalrealty.ca/properties']
    allowed_domains = ["royalrealty.ca"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
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
        apartments_divs = response.css('div.block.property')
        for apartment_div in apartments_divs:
            address = apartment_div.css('header h2 span.address::text').extract()
            title = apartment_div.css('section h2::text').extract()
            description = apartment_div.css('section p.description::text').extract()
            url = apartment_div.css('section a::attr(href)')[0].extract()
            yield scrapy.Request(url, callback=self.populate_item, meta={
                "address": address,
                "title": title,
                "description": description,
            })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        apartment_info = response.css('section#content div')
        suites = apartment_info.xpath('.//table[contains(@id, "apartment-types")]//tbody//tr')
        limit = len(suites) / 2
        suites = suites[:int(limit)]
        for suite in suites:
            rent = suite.xpath('.//td[3]/text()').extract()
            rent = rent[0]
            rent = rent.split("$")[1]
            rent = rent.replace(",", "")
            rent = float(rent)
            rent = round(rent)
            rent = int(rent)
            if rent != 0:
                item_loader = ListingLoader(response=response)
                suite_type = suite.xpath('.//td[1]/text()')[0].extract()

                address = response.meta.get("address")
                address = address[0] + ", Canada"
                longitude, latitude = extract_location_from_address(address)
                longitude = str(longitude)
                latitude = str(latitude)
                zipcode, city, no_address = extract_location_from_coordinates(longitude, latitude)

                title = response.meta.get("title")
                title = title[0] + " | " + suite_type

                description_one = response.meta.get("description")
                description_two = apartment_info.css('p::text').extract()
                description_two_list = []
                for item in description_two:
                    item = item.strip()
                    if "call" not in item:
                        description_two_list.append(item)
                description_two_list = " ".join(description_two_list)
                description = description_one[0] + description_two_list
                description = description.replace("\t", "")
                description = description.replace("\n", "")

                room_count = suite_type.split()[0]
                room_count = int(room_count)

                suite_images = suite.xpath('.//td[2]//a//ul//li/@data-large-src').extract()
                building_images = response.css('ul#photo-preview li img::attr(src)').extract()
                images = building_images + suite_images

                landlord_name = apartment_info.css('div.column-left div#resident-manager span.manager::text')[0].extract()
                landlord_number = apartment_info.css('div.column-left div#resident-manager span.phone::text')[0].extract()

                suite_features = response.css('div#features ul li::text').extract()
                washing_machine = None
                parking = None
                dishwasher = None
                for item in suite_features:
                    if "parking" in item.lower():
                        parking = True
                    if "Washer" in item:
                        washing_machine = True
                    if "dishwasher" in item.lower():
                        dishwasher = True

                suite_link = suite_type.strip()
                suite_link = suite_link.replace(" ", "-")
                external_link = response.url + "#" + suite_link

                property_type = "apartment"

                # # MetaData
                item_loader.add_value("external_link", external_link) # String
                item_loader.add_value("external_source", self.external_source) # String

                #item_loader.add_value("external_id", external_id) # String
                item_loader.add_value("position", self.position) # Int
                item_loader.add_value("title", title) # String
                item_loader.add_value("description", description) # String

                # # Property Details
                item_loader.add_value("city", city) # String
                item_loader.add_value("zipcode", zipcode) # String
                item_loader.add_value("address", address) # String
                item_loader.add_value("latitude", latitude) # String
                item_loader.add_value("longitude", longitude) # String
                #item_loader.add_value("floor", floor) # String
                item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
                #item_loader.add_value("square_meters", square_meters) # Int
                item_loader.add_value("room_count", room_count) # Int
                #item_loader.add_value("bathroom_count", bathroom_count) # Int

                #item_loader.add_value("available_date", available_date) # String => date_format

                #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                #item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking) # Boolean
                #item_loader.add_value("elevator", elevator) # Boolean
                #item_loader.add_value("balcony", balcony) # Boolean
                #item_loader.add_value("terrace", terrace) # Boolean
                #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                item_loader.add_value("washing_machine", washing_machine) # Boolean
                item_loader.add_value("dishwasher", dishwasher) # Boolean

                # # Images
                item_loader.add_value("images", images) # Array
                item_loader.add_value("external_images_count", len(images)) # Int
                #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
                #item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()
