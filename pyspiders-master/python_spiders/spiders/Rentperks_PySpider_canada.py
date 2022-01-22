# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from urllib.request import urlopen
import json
from ..helper import extract_location_from_coordinates, extract_location_from_address


class RentperksPyspiderCanadaSpider(scrapy.Spider):
    name = "Rentperks"
    start_urls = [
        'https://rentperks.com/frontend/tenant/filter?city=&page_no=1&price_range=&price_val_from=&price_val_to=&bedroom=0&bathroom=0&read_more=&agent=1&latitude=&longitude=',
        'https://rentperks.com/frontend/tenant/filter?city=&page_no=2&price_range=&price_val_from=&price_val_to=&bedroom=0&bathroom=0&read_more=&agent=1&latitude=&longitude=',
        'https://rentperks.com/frontend/tenant/filter?city=&page_no=3&price_range=&price_val_from=&price_val_to=&bedroom=0&bathroom=0&read_more=&agent=1&latitude=&longitude=',
        'https://rentperks.com/frontend/tenant/filter?city=&page_no=4&price_range=&price_val_from=&price_val_to=&bedroom=0&bathroom=0&read_more=&agent=1&latitude=&longitude=',
                  ]
    allowed_domains = ["rentperks.com"]
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
        response = urlopen(response.url)
        apartments_json = json.loads(response.read())
        apartments_json = apartments_json["result"]
        for apartment_json in apartments_json:
            external_id = apartment_json["property_id"]
            apartment_link = apartment_json["property_pub_id"]
            description = apartment_json["description"]
            address = apartment_json["address"]
            street = apartment_json["address_optional"]
            area = apartment_json["area"]
            latitude = apartment_json["latitude"]
            longitude = apartment_json["longitude"]
            available_date = apartment_json["available_from"]
            rent = apartment_json["rent"]
            room_count = apartment_json["bedroom"]
            bathroom_count = apartment_json["bathroom"]
            parking = apartment_json["parking"]
            apartment_info = {
                "external_id": external_id,
                "description": description,
                "address": address,
                "street": street,
                "area": area,
                "latitude": latitude,
                "longitude": longitude,
                "available_date": available_date,
                "rent": rent,
                "room_count": room_count,
                "bathroom_count": bathroom_count,
                "parking": parking,
            }
            apartment_url = "https://rentperks.com/share/property/" + apartment_link + ".html"
            yield scrapy.Request(url=apartment_url, callback=self.populate_item, meta=apartment_info)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        external_id = response.meta.get("external_id")

        available_date = response.meta.get("available_date")

        room_count = response.meta.get("room_count")
        room_count = float(room_count)
        room_count = round(room_count)
        room_count = int(room_count)

        bathroom_count = response.meta.get("bathroom_count")
        bathroom_count = float(bathroom_count)
        bathroom_count = round(bathroom_count)
        bathroom_count = int(bathroom_count)

        parking = response.meta.get("parking")
        if parking.isdigit():
            if int(parking) > 0:
                parking = True
            else:
                parking = None
        else:
            parking = None

        rent = float(response.meta.get("rent"))
        rent = round(rent)
        rent = int(rent)

        address = response.xpath('.//p[contains(@class, "share-prop-full-addr")]/text()').extract()
        address = ", ".join(address)
        longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)
        zipcode, city, no_address = extract_location_from_coordinates(longitude, latitude)

        images = response.xpath('.//div[contains(@class, "image-section")]//img/@src').extract()

        description = response.xpath('normalize-space(.//p[contains(@class, "share-prop-desc")]/text())').extract()

        landlord_name = "Rent Perks"
        tel_email = response.xpath('.//div[contains(@class, "share-banner-main-right")]//a/@href').extract()
        landlord_number = None
        landlord_email = None
        for item in tel_email:
            if item.startswith("tel:"):
                landlord_number = item
                landlord_number = landlord_number.replace("tel:", "")
            if item.startswith("mailto:"):
                landlord_email = item
                landlord_email = landlord_email.replace("mailto:", "")

        unit_details = response.xpath('.//div[contains(@class, "share-banner-left")]')
        title = unit_details.xpath('.//h1/text()').extract()

        square_meters = unit_details.xpath('.//ul//li[3]/text()')[0].extract()
        square_meters = square_meters.split(".")
        square_meters = float(square_meters[0])
        square_meters = round(square_meters)
        square_meters = int(square_meters)
        if square_meters == 0:
            square_meters = None

        property_type = unit_details.xpath('.//ul//li[5]/text()')[0].extract()
        if property_type in ['Flat/Apartment', 'Duplex', 'Condo']:
            property_type = "apartment"
        elif "home" in property_type.lower():
            property_type = "house"
        elif "townhouse" in property_type.lower():
            property_type = "house"
        elif "basement suite" in property_type.lower():
            property_type = "apartment"

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
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
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

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
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
