# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_location_from_address
from xml.etree import ElementTree
from lxml.etree import XMLParser
from parsel import Selector
import requests


class HrpmPyspiderCanadaSpider(scrapy.Spider):
    name = "HRPM"
    start_urls = ['https://hrpm.ca/hr-communities-gta-map-address/']
    allowed_domains = ["hrpm.ca"]
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
        # Your code goes here
        apartments_divs = response.xpath('.//div[contains(@class, "col-lg-6")]')
        for apartment_div in apartments_divs:
            apartment_url = apartment_div.xpath('.//a/@href')[0].extract()
            title = apartment_div.xpath('.//a//h2/text()')[0].extract()
            title = title.replace("Apartment for rent at", "")
            address = apartment_div.xpath('.//a//div[contains(@class, "cardAddress")]/text()').extract()
            yield scrapy.Request(url=apartment_url, callback=self.populate_item, meta={"title": title, "address": address})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        # 1. Data from original url (each building url)
        building_title = response.meta.get("title")
        address = response.meta.get("address")
        address = ", ".join(address)
        address = address.strip()

        property_type = "apartment"

        amenities = response.xpath('.//div[contains(@class, "amenities")]//div[contains(@class, "active")]/text()').extract()
        elevator = None
        swimming_pool = None
        parking = None
        for amenity in amenities:
            if "elevators" in amenity.lower():
                elevator = True
            if "pool" in amenity.lower():
                swimming_pool = True
            if "parking" in amenity.lower():
                parking = True

        floor_plan_images = response.xpath('.//div[contains(@class, "floorPlans")]//a[contains(@class, "plan")]/@href').extract()
        images = response.xpath('.//div[contains(@class, "carousel-inner")]//a/@href').extract()

        # 2. Data from hr_buildings xml
        building_response = requests.get("https://hrpm.ca/hrXmlExport/hr_buildings.xml")
        parser = XMLParser(strip_cdata=False)
        root = ElementTree.fromstring(building_response.content, parser=parser)
        selector = Selector(root=root)
        buildings_rows = selector.xpath('.//row')

        # 3. Data from hr_units xml
        building_response = requests.get("https://hrpm.ca/hrXmlExport/hr_units.xml")
        parser = XMLParser(strip_cdata=False)
        root = ElementTree.fromstring(building_response.content, parser=parser)
        selector = Selector(root=root)
        units_rows = selector.xpath('.//row')

        # 4. Data from hr_unitType xml
        building_response = requests.get("https://hrpm.ca/hrXmlExport/unitTypes.xml")
        parser = XMLParser(strip_cdata=False)
        root = ElementTree.fromstring(building_response.content, parser=parser)
        selector = Selector(root=root)
        unit_type_rows = selector.xpath('.//row')

        # 5. Looping in buildings list
        # filter problem
        for row in buildings_rows:
            building_url = row.xpath('.//field[contains(@name, "Build_Address")]/text()')[0].extract()
            building_url = building_url.lower()
            building_url = building_url.replace(" ", "-")
            building_url = building_url.replace(".", "")
            main_building_url = (response.url.split("/"))[-2:]
            for building_url_item in main_building_url:
                if building_url_item.startswith(building_url):
                    building_id = row.xpath('.//field[contains(@name, "Build_ID")]/text()')[0].extract()
                    building_description = row.xpath('.//field[contains(@name, "Build_Desc")]/text()')[0].extract()
                    landlord_number = row.xpath('.//field[contains(@name, "Build_Contact_Phone")]/text()')[0].extract()
                    landlord_email = row.xpath('.//field[contains(@name, "Build_Contact_Email")]/text()')[0].extract()
                    landlord_name = "H & R Property Management Limited"

                    longitude, latitude = extract_location_from_address(address)
                    longitude = str(longitude)
                    latitude = str(latitude)
                    zipcode, city, no_address = extract_location_from_coordinates(longitude, latitude)

                    for unit in units_rows:
                        building_id_units = unit.xpath('.//field[contains(@name, "Build_ID")]/text()')[0].extract()
                        if building_id_units == building_id:
                            unit_id = unit.xpath('.//field[contains(@name, "UT_ID")]/text()')[0].extract()
                            external_id = unit.xpath('.//field[contains(@name, "Un_ID")]/text()')[0].extract()
                            rent = unit.xpath('.//field[contains(@name, "Un_Price")]/text()')[0].extract()
                            rent = int(rent)
                            unit_description = unit.xpath('.//field[contains(@name, "Un_Desc")]/text()')[0].extract()

                            if "call" in building_description.lower():
                                building_description = building_description[:((building_description.lower()).index("call"))]
                            if "call" in unit_description.lower():
                                unit_description = unit_description[:((unit_description.lower()).index("call"))]
                            description = building_description.strip() + " " + unit_description.strip()

                            for unit_type in unit_type_rows:
                                unit_type_id = unit_type.xpath('.//field[contains(@name, "UT_ID")]/text()')[0].extract()
                                if unit_type_id == unit_id:
                                    unit_title = unit_type.xpath('.//field[contains(@name, "UT_Desc")]/text()')[0].extract()
                                    title = building_title + " - " + unit_title
                                    room_count = unit_title.split()
                                    if "Penthouse" in room_count:
                                        room_count = 1
                                    elif "Bachelor" in room_count:
                                        room_count = 1
                                    elif "One" in room_count:
                                        room_count = 1
                                    else:
                                        room_count = self.convert_string_into_number(room_count[0])

                                    external_link = response.url + "#" + unit_title.replace(" ", "-")

                                    item_loader = ListingLoader(response=response)

                                    # # MetaData
                                    item_loader.add_value("external_link", external_link)  # String
                                    item_loader.add_value("external_source", self.external_source)  # String

                                    item_loader.add_value("external_id", external_id)  # String
                                    item_loader.add_value("position", self.position)  # Int
                                    item_loader.add_value("title", title)  # String
                                    item_loader.add_value("description", description)  # String

                                    # # Property Details
                                    item_loader.add_value("city", city)  # String
                                    item_loader.add_value("zipcode", zipcode)  # String
                                    item_loader.add_value("address", address)  # String
                                    item_loader.add_value("latitude", latitude)  # String
                                    item_loader.add_value("longitude", longitude)  # String
                                    # item_loader.add_value("floor", floor) # String
                                    item_loader.add_value("property_type", property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
                                    # item_loader.add_value("square_meters", square_meters)  # Int
                                    item_loader.add_value("room_count", room_count)  # Int
                                    # item_loader.add_value("bathroom_count", bathroom_count)  # Int
                                    #
                                    # item_loader.add_value("available_date", available_date)  # String => date_format

                                    # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                                    # item_loader.add_value("furnished", furnished) # Boolean
                                    item_loader.add_value("parking", parking)  # Boolean
                                    item_loader.add_value("elevator", elevator) # Boolean
                                    # item_loader.add_value("balcony", balcony) # Boolean
                                    # item_loader.add_value("terrace", terrace) # Boolean
                                    item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                                    # item_loader.add_value("washing_machine", washing_machine) # Boolean
                                    # item_loader.add_value("dishwasher", dishwasher) # Boolean

                                    # # Images
                                    item_loader.add_value("images", images)  # Array
                                    item_loader.add_value("external_images_count", len(images))  # Int
                                    item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
                                    item_loader.add_value("landlord_name", landlord_name)  # String
                                    item_loader.add_value("landlord_phone", landlord_number)  # String
                                    item_loader.add_value("landlord_email", landlord_email)  # String

                                    self.position += 1
                                    yield item_loader.load_item()

    def convert_string_into_number(self, string_number):
        numbers = {
            "one": 1,
            "two": 2,
            "three": 3,
            "four": 4,
            "five": 5,
            "six": 6,
        }
        return numbers.get(str(string_number).lower())
