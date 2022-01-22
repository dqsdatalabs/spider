# -*-    coding: utf-8 -*-
# Author: Abdulrahman Abbas
import json

import scrapy

from ..helper import string_found, extract_location_from_coordinates
from ..loaders import ListingLoader


class MintoapartmentsCaSpider(scrapy.Spider):
    name = "mintoapartments_ca"
    start_urls = [
        'https://www.minto.com/mg_overview.php?action=get_locations_and_zones&category=apartment&city=ottawa&lang=undefined',
        'https://www.minto.com/mg_overview.php?action=get_locations_and_zones&category=apartment&city=toronto&lang=undefined',
        'https://www.minto.com/mg_overview.php?action=get_locations_and_zones&category=apartment&city=calgary&lang=undefined',
        'https://www.minto.com/mg_overview.php?action=get_locations_and_zones&category=apartment&city=montreal&lang=undefined',
        'https://www.minto.com/mg_overview.php?action=get_locations_and_zones&category=apartment&city=london&lang=undefined',
        'https://www.minto.com/mg_overview.php?action=get_locations_and_zones&category=apartment&city=edmonton&lang=undefined'
    ]

    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "minto_apartments_ca_pyspider_canada_en"
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        data = json.loads(response.body)
        new_data = data["locations"]
        for item in new_data:
            urls = item["prjmain_url"]
            description = item["Description"]
            yield scrapy.Request(url=urls, callback=self.populate_item,
                                 meta={
                                     "item": item,
                                     "description": description,
                                 })

    def populate_item(self, response):

        data = response.meta['item']
        landlord_phone = data["Phone"]
        landlord_name = data["Name"]
        landlord_email = data["Email"]
        title1 = data["Seo_title1"]
        longitude = data["Longitude"]
        latitude = data["Latitude"]
        address = data["Address"]
        zipcode = data["Postal"]
        city = data["City"]

        if not address.isalpha():
            address = extract_location_from_coordinates(longitude, latitude)[-1]

        if not zipcode.isalpha():
            zipcode = extract_location_from_coordinates(longitude, latitude)[0]

        description = data["Description"].replace("Inquire by email", "")
        images = response.xpath(
            '//section[@class="container-fluid community-content py-5 locgallery global-section-grey"]//a//@href').getall()

        for image in images:
            if "rent_your_way" in image:
                images.remove(image)
                for img in images:
                    if "rent_your_way" in img:
                        images.remove(img)

        details1 = response.xpath(
            '//section[@class="container-fluid community-content pb-5 locamenities-features locaf-building pt-5"]//div[@class="row"]//ul//text()').getall()
        details2 = response.xpath(
            '//section[@class="container-fluid community-content pb-5 locamenities-features locaf-unit"]//ul//text()').getall()

        amenities = " ".join(details1) + " ".join(details2)

        parking = False
        if string_found(['parking'], amenities):
            parking = True

        balcony = False
        if string_found(['balcony'], amenities):
            balcony = True

        dishwasher = False
        if string_found(['dishwasher'], amenities):
            dishwasher = True

        pets_allowed = False
        if string_found(['Pet Friendly'], amenities):
            pets_allowed = True

        washing_machine = False
        if string_found(['laundry'], amenities):
            washing_machine = True

        elevator = False
        if string_found(['elevator'], amenities):
            elevator = True

        swimming_pool = False
        if string_found(['pool'], amenities):
            swimming_pool = True

        terrace = False
        if string_found(['terrace'], amenities):
            terrace = True

        furnished = False
        if string_found(['furnished'], amenities):
            furnished = True

        units = data["Units"]

        for unit in units:
            if unit["Waiting_list"] == "no":
                external_id = unit["ID_Unit"]
                title = unit["Name"]
                rent = unit["Last_price_drop_newprice"]

                room_count = unit["Beds"]
                if unit["Beds"] is not None and ".5" in unit["Beds"]:
                    room_count = float(unit["Beds"])
                    room_count += 0.5

                if "0" in unit["Beds"]:
                    room_count = 1

                bathroom_count = unit["Baths"]
                if unit["Baths"] is not None and ".5" in unit["Baths"]:
                    bathroom_count = float(unit["Baths"])
                    bathroom_count += 0.5

                available_date = None
                if unit["Move_in_date"] != "Available now" or unit["Move_in_date"] != "Available Now":
                    available_date = unit["Move_in_date"]

                # # MetaData
                if "fifthandbankliving" not in response.url and rent != "0":
                    item_loader = ListingLoader(response=response)
                    item_loader.add_value("external_link", f"{response.url}/#{external_id}")  # String
                    item_loader.add_value("external_source", self.external_source)  # String

                    item_loader.add_value("external_id", str(external_id))  # String
                    item_loader.add_value("position", self.position)  # Int
                    item_loader.add_value("title", f"{title}-{title1}")  # String
                    item_loader.add_value("description", description)  # String

                    # # Property Details
                    item_loader.add_value("city", city)  # String
                    item_loader.add_value("zipcode", zipcode)  # String
                    item_loader.add_value("address", address)  # String
                    item_loader.add_value("latitude", str(latitude))  # String
                    item_loader.add_value("longitude", str(longitude))  # String
                    # item_loader.add_value("floor", floor) # String
                    item_loader.add_value("property_type", 'apartment')  # String
                    # item_loader.add_value("square_meters", square_meters) # Int
                    item_loader.add_value("room_count", room_count)  # Int
                    item_loader.add_value("bathroom_count", bathroom_count)  # Int

                    item_loader.add_value("available_date", available_date)  # String => date_format

                    item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
                    item_loader.add_value("furnished", furnished)  # Boolean
                    item_loader.add_value("parking", parking)  # Boolean
                    item_loader.add_value("elevator", elevator)  # Boolean
                    item_loader.add_value("balcony", balcony)  # Boolean
                    item_loader.add_value("terrace", terrace)  # Boolean
                    item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
                    item_loader.add_value("washing_machine", washing_machine)  # Boolean
                    item_loader.add_value("dishwasher", dishwasher)  # Boolean

                    # # Images
                    floor_plan_images = response.xpath(
                        '//section[@id="rates_floorplans"]//tbody//td[@class="border-0 floorplan align-middle"]//a//@href').get()

                    item_loader.add_value("images", images)  # Array
                    item_loader.add_value("external_images_count", len(images))  # Int
                    item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

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
                    item_loader.add_value("landlord_phone", landlord_phone)  # String
                    item_loader.add_value("landlord_email", landlord_email)  # String

                    self.position += 1
                    yield item_loader.load_item()
