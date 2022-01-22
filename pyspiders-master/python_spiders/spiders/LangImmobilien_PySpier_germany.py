# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address
import urllib.request
import json


class LangimmobilienPyspierGermanySpider(scrapy.Spider):
    name = "LangImmobilien"
    start_urls = ['https://www.langimmobilien.de/immobilienangebote/mieten/']
    allowed_domains = ["langimmobilien.de"]
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
        pages_url = response.xpath('.//li[contains(@class, "pagebrowser-pagelink")]//a/text()').extract()
        pages_count = pages_url[-1]
        pages_count = int(pages_count)
        urls_list = []
        for page_number in range(pages_count):
            next_page = page_number + 1
            url = "https://www.langimmobilien.de/immobilienangebote/mieten/seite/" + str(next_page) + "/"
            urls_list.append(url)
        for url in urls_list:
            yield scrapy.Request(url, callback=self.parse_pages)

    def parse_pages(self, response):
        apartments_divs = response.xpath('.//div[contains(@class, "offer")]')
        for apartment_div in apartments_divs:
            apartment_url = apartment_div.xpath('.//a[contains(@class, "exposeLink")]/@href').extract()
            url = "https://www.langimmobilien.de/" + apartment_url[0]
            rent = apartment_div.xpath('.//div[contains(@class, "price")]/text()')[0].extract()
            room_count = apartment_div.xpath('.//div[contains(@class, "rooms")]/text()')[0].extract()
            square_meters = apartment_div.xpath('.//div[contains(@class, "area")]/text()').extract()
            city = apartment_div.xpath('.//div[contains(@class, "city")]/text()')[0].extract()
            yield scrapy.Request(url, callback=self.populate_item, meta={
                "rent": rent,
                "room_count": room_count,
                "square_meters": square_meters,
                "city": city,
            })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        info_div = response.xpath('.//div[contains(@class, "nb-maklerTool-expose-box-holder")]')
        apartment_info = info_div.xpath('.//div[contains(@class, "nb-maklerTool-expose-box-holder-leftcol")]')
        apartment_key_data = apartment_info.xpath('.//div[contains(@class, "nb-maklerTool-expose-box")]//table//tr')
        apartment_key_data_keys = apartment_key_data.xpath('.//td[contains(@class, "label")]//text()').extract()
        apartment_key_data_values = apartment_key_data.xpath('.//td[contains(@class, "value")]//text()').extract()
        apartment_key_data_dict = dict(zip(apartment_key_data_keys, apartment_key_data_values))

        property_type = apartment_key_data_dict["Immobilienart:"]
        item_loader = ListingLoader(response=response)

        if "Büro" not in property_type:
            if "wohnung" in property_type:
                property_type = "apartment"
            elif "apartment" in property_type:
                property_type = "apartment"
            elif "Souterrain" in property_type:
                property_type = "apartment"

            external_id = apartment_key_data_dict["Objekt-ID:"]

            address = apartment_key_data_dict["Lage:"]

            zipcode = (address.split())[0]

            longitude, latitude = extract_location_from_address(address)
            longitude = str(longitude)
            latitude = str(latitude)

            bathroom_count = None
            floor = None
            utilities = None
            deposit = None
            parking = None
            available_date = None
            if "Badezimmer:" in apartment_key_data_dict.keys():
                bathroom_count = apartment_key_data_dict["Badezimmer:"]
                bathroom_count = bathroom_count.split()[0]
                bathroom_count = float(bathroom_count)
                bathroom_count = round(bathroom_count)
                bathroom_count = int(bathroom_count)

            if "Etage:" in apartment_key_data_dict.keys():
                floor = apartment_key_data_dict["Etage:"]

            if "Nebenkosten:" in apartment_key_data_dict.keys():
                utilities = apartment_key_data_dict["Nebenkosten:"]
                utilities = (utilities.split())[0]
                utilities = utilities.replace(".", "")
                utilities = utilities.replace(",", ".")
                utilities = float(utilities)
                utilities = round(utilities)
                utilities = int(utilities)

            if "Kaution:" in apartment_key_data_dict.keys():
                deposit = apartment_key_data_dict["Kaution:"]
                deposit = (deposit.split())[0]
                deposit = deposit.replace(".", "")
                deposit = deposit.replace(",", ".")
                deposit = float(deposit)
                deposit = round(deposit)
                deposit = int(deposit)

            if "Tiefgaragenstellplätze:" in apartment_key_data_dict.keys():
                parking = True

            if "Bezugstermin:" in apartment_key_data_dict.keys():
                available_date = apartment_key_data_dict["Bezugstermin:"]
                if "." not in available_date:
                    available_date = None
                else:
                    available_date = available_date.split(".")
                    day = available_date[0]
                    month = available_date[1]
                    year = available_date[2]
                    available_date = year.strip() + "-" + month.strip() + "-" + day.strip()

            rent = response.meta.get("rent")
            rent = (rent.split())[0]
            rent = rent.replace(".", "")
            rent = rent.replace(",", ".")
            rent = float(rent)
            rent = round(rent)
            rent = int(rent)

            room_count = response.meta.get("room_count")
            room_count = room_count.split()[0]
            room_count = float(room_count)
            room_count = round(room_count)
            room_count = int(room_count)

            square_meters = response.meta.get("square_meters")
            if len(square_meters) >= 1:
                square_meters = square_meters[0]
                square_meters = (square_meters.split())[0]
                square_meters = int(square_meters)
            else:
                square_meters = None

            city = response.meta.get("city")

            title = response.xpath('.//div[contains(@class, "nb-maklerTool-expose-name")]//h1/text()')[0].extract()

            description = apartment_info.xpath('.//div[contains(@id, "description")]//div[contains(@class, "accContent")]/text()').extract()

            amenities = apartment_info.xpath('.//div[contains(@id, "location")]//div[contains(@class, "accContent")]/text()').extract()

            if description == "true":
                description = None
                amenities = None

            balcony = None
            washing_machine = None
            terrace = None
            elevator = None
            if amenities:
                for item in amenities:
                    if "Balkon" in item:
                        balcony = True
                    if "Pkw-Stellplatz" in item:
                        parking = True
                    if "Waschmaschine" in item:
                        washing_machine = True
                    if "Terrasse" in item:
                        terrace = True
                    if "Aufzug" in item:
                        elevator = True

            landlord_name = "Michael Lang"
            landlord_number = "069 9200250"
            landlord_email = "info@langimmobilien.de"

            images_url = "https://www.langimmobilien.de/index.php?eID=nb_maklertool&action=pic&obj=" + external_id + "&width=940c&height=532c&expose=1"
            images_response = urllib.request.urlopen(images_url)
            images_json = images_response.read()
            images_json = json.loads(images_json)
            images = []
            floor_plan_images = []
            for image in images_json:
                if image[1] == "Die Aufteilung":
                    floor_plan_image = image[0]
                    image_url = floor_plan_image.replace("\\", "")
                    image_url = "https://www.langimmobilien.de/" + image_url
                    floor_plan_images.append(image_url)
                else:
                    image_url = image[0]
                    image_url = image_url.replace("\\", "")
                    image_url = "https://www.langimmobilien.de/" + image_url
                    images.append(image_url)

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
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            #item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            #item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_number) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
