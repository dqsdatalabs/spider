# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates,extract_location_from_address


class VereinigteleipzigerwohnungsgenossenschaftPyspiderGermanySpider(scrapy.Spider):
    name = "VereinigteLeipzigerWohnungsgenossenschaft"
    start_urls = ['https://vlw-eg.de/suchergebnisse']
    allowed_domains = ["vlw-eg.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
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
        apartments_divs = response.xpath('.//div[contains(@class, "estate-result-list")]//div[contains(@class, "estate-item")]')
        for apartment_div in apartments_divs:
            url = apartment_div.xpath('.//div[contains(@class, "row")]//div[contains(@class, "image-wrapper")]//a/@href')[0].extract()
            rent = apartment_div.xpath('.//div[contains(@class, "row")]//p[contains(@class, "price")]/text()').extract()
            title = apartment_div.xpath('.//div[contains(@class, "row")]//h4[contains(@class, "heading_h4")]/text()').extract()
            apartment_info = apartment_div.xpath('.//div[contains(@class, "col-xs-12")]//div[contains(@class, "col-xs-12")]//p/text()').extract()
            yield scrapy.Request(url, callback=self.populate_item, meta={
                "rent": rent,
                "title": title,
                "apartment_info": apartment_info,
            })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        property_exist = response.xpath('.//h5/text()').extract()
        if "keine Mietobjekte entsprechend Ihrer Kriterien gefunden" not in property_exist:
            item_loader = ListingLoader(response=response)

            title = response.meta.get("title")
            title = title[0]

            apartment_info = response.meta.get("apartment_info")

            square_meters = apartment_info[1]
            square_meters = (square_meters.split())[0]
            square_meters = int(square_meters)

            floor = apartment_info[2]
            floor = floor.strip()
            if floor == "Erdgeschoss":
                floor = "Erdgeschoss"
            else:
                floor = (floor.split())[0]
                floor = floor.replace(".", "")

            room_count = apartment_info[3]
            room_count = (room_count.split())[0]
            room_count = float(room_count)
            room_count = round(room_count)
            room_count = int(room_count)

            available_date = apartment_info[4]
            available_date = available_date.split(".")
            day = available_date[0]
            month = available_date[1]
            year = available_date[2]
            available_date = year.strip() + "-" + month.strip() + "-" + day.strip()

            address = response.xpath('.//p[contains(@class, "address")]/text()')[0].extract()
            longitude, latitude = extract_location_from_address(address)
            longitude = str(longitude)
            latitude = str(latitude)
            zipcode, city, no_address = extract_location_from_coordinates(longitude, latitude)

            images = []
            images_raw = response.xpath('.//img[contains(@class, "image")]/@src').extract()
            for image in images_raw:
                if "<br" not in image:
                    images.append("https://vlw-eg.de" + image)

            description = response.xpath('normalize-space(.//p[contains(@class, "description")]/text())').extract()

            external_id = response.url.split("id=")[1]

            property_type = "apartment"

            floor_plan_images = []
            floor_plan_images_raw = response.xpath('.//div[contains(@class, "estateitems")]//img[contains(@class, "img-responsive")]')
            for image in floor_plan_images_raw:
                alt_path = image.xpath('./@alt')[0].extract()
                if alt_path.startswith("Grundriss"):
                    floor_plan_image = image.xpath('./@src')[0].extract()
                    floor_plan_images.append("https://vlw-eg.de" + floor_plan_image)

            apartment_info = response.xpath('.//div[contains(@class, "estate-menu-item")]')
            rent_room_keys = apartment_info.xpath('.//div[contains(@class, "sidebar-elements")]//dl//dt/text()').extract()
            rent_room_values = apartment_info.xpath('.//div[contains(@class, "sidebar-elements")]//dl//dd/text()').extract()
            rent_room = dict(zip(rent_room_keys, rent_room_values))

            rent = rent_room["Kaltmiete"]
            rent = rent.replace("€", "")
            rent = rent.replace(",", ".")
            rent = float(rent)
            rent = round(rent)
            rent = int(rent)

            utilities = rent_room["Heiz- & Nebenkosten:"]
            utilities = utilities.replace("€", "")
            utilities = utilities.replace(",", ".")
            utilities = float(utilities)
            utilities = round(utilities)
            utilities = int(utilities)

            energy_label = None
            if "Energieeffizienz:" in rent_room.keys():
                energy_label = rent_room["Energieeffizienz:"]

            amenities = apartment_info.xpath('.//div[contains(@class, "sidebar-elements")]/text()').extract()
            balcony = None
            for item in amenities:
                if "Balkon" in item:
                    balcony = True

            landlord_name_number = response.xpath('.//div[contains(@class, "col-xs-7")]//p/text()').extract()
            landlord_name = landlord_name_number[0]
            landlord_number = landlord_name_number[1]
            landlord_number = landlord_number.replace("/", " ")
            landlord_email = 'info@vlw-eg.de'

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
            item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            #item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            #item_loader.add_value("parking", parking) # Boolean
            #item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            #item_loader.add_value("washing_machine", washing_machine) # Boolean
            #item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            #item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_number) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
