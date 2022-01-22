# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address, extract_location_from_coordinates


class BruningimmobilienPyspiderGermanySpider(scrapy.Spider):
    name = "Bruningimmobilien"
    start_urls = [
        'https://bruening-immo.de/kehl/index.php?SPR=DE&ORT=KE&KAMI=MIET&REG=ALLE&OBJ=HAUS',
        'https://bruening-immo.de/kehl/index.php?SPR=DE&ORT=KE&KAMI=MIET&REG=ALLE&OBJ=WOHN'
        ]
    allowed_domains = ["bruening-immo.de"]
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
        apartments_divs = response.xpath('.//div[contains(@class, "flagge")]')
        for apartment_div in apartments_divs:
            availability = apartment_div.xpath('.//div[contains(@class, "Hinweis_Schild_g")]//span//font//font/text()').extract()
            if availability:
                availability = availability[0].split()
            if "vermietet" not in availability:
                # room_count = apartment_div.xpath('normalize-space(.//div[contains(@class, "Zimmer")]/text())').extract()
                room_count = apartment_div.css('div.Zimmer::text').extract()
                if len(room_count) > 1:
                    room_count = room_count[0].strip()
                external_id = apartment_div.xpath('.//div[contains(@class, "NrSet")]/text()').extract()
                rent = apartment_div.xpath('.//div[contains(@class, "PrSet")]/text()').extract()
                title = apartment_div.xpath('.//div[contains(@class, "das")]/text()').extract()
                square_meters = apartment_div.xpath('.//span[contains(@class, "qm")]/text()').extract()
                apartment_url = apartment_div.xpath('.//a/@href').extract()
                url = "https://bruening-immo.de/kehl/" + apartment_url[0]
                yield scrapy.Request(url=url, callback=self.populate_item,
                                     meta={
                                         "room_count": room_count,
                                         "external_id": external_id,
                                         "rent": rent,
                                         "title": title,
                                         "square_meters": square_meters,
                                     })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        if response.xpath('.//div[contains(@class, "balken flex")]'):
            item_loader = ListingLoader(response=response)

            room_count = response.meta.get("room_count")
            if len(room_count) > 0:
                room_count = room_count.split()
                room_count = room_count[0]
                room_count = int(room_count)
            else:
                room_count = None

            external_id = response.meta.get("external_id")
            external_id = (external_id[0])
            external_id = external_id.replace("Nr.", "")
            external_id = external_id.strip()

            rent = response.meta.get("rent")
            rent = rent[0]
            rent = rent.split(",")
            rent = rent[0]
            rent = rent.replace(".", "")
            rent = [int(s) for s in rent.split() if s.isdigit()]

            title = response.meta.get("title")
            property_type = None
            for item in title[0].split():
                if "Einfamilienhaus" in item:
                    property_type = "house"
                    break
                elif "Einfamilienhaus" not in item:
                    property_type = "apartment"

            square_meters = response.meta.get("square_meters")
            square_meters = (square_meters[0].split(":"))[1]
            square_meters = [int(s) for s in square_meters.split() if s.isdigit()]
            description = response.xpath('.//p[contains(@class, "abschnittA")]/text()').extract()

            balcony = None
            parking = None
            terrace = None
            elevator = None
            energy_label = None
            available_date = None
            utilities_items = None
            deposit_items = None
            deposit = None
            rooms_list = []
            room_count_all = []
            all_rooms = response.xpath('.//p[contains(@class, "ntextU")]/text()').extract()
            for room in all_rooms:
                if "Balkon" in room:
                    balcony = True
                if "Kfz-Stellplatz" in room:
                    parking = True
                if "Dachterrasse" in room:
                    terrace = True
                if "Schlafzimmer" in room:
                    rooms_list.append(room)

            if not room_count:
                for room_list in rooms_list:
                    for item in room_list.split():
                        if item.isdigit():
                            room_count_all.append(int(item))
                            room_count = sum(room_count_all)

            apartment_info = response.xpath('.//div[contains(@class, "oliste")]//ul//li/text()').extract()
            for item in apartment_info:
                if "dachterrasse" in item.lower():
                    terrace = True
                if "aufzug" in item.lower():
                    elevator = True
                if "tiefgaragenstellplatz" in item.lower():
                    parking = True
                if "energieeffizienzklasse" in item.lower():
                    energy_label = item.split(":")[1]
                if "frei ab" in item.lower():
                    available_date = item
                if "nebenkosten" in item.lower():
                    if "-" in item:
                        utilities_items = item.split("-")[0]
                    else:
                        utilities_items = item
                if "kaution" in item.lower():
                    if "-" in item:
                        deposit_items = item.split("-")[1]
                    else:
                        deposit_items = item

                deposit_list = []
                if deposit_items:
                    for item in deposit_items.split():
                        item = item.replace(".", "")
                        if item.isdigit():
                            deposit_list.append(item)
                if len(deposit_list) == 2:
                    deposit = int(deposit_list[1])
                elif len(deposit_list) == 1:
                    if len(deposit_list[0]) == 1:
                        deposit = rent[0] * int(deposit_list[0])
                    else:
                        deposit = int(deposit_list[0])

                utilities_list = []
                utilities = None
                if utilities_items:
                    for item in utilities_items.split():
                        if item.isdigit():
                            utilities_list.append(item)
                if len(utilities_list) == 1:
                    utilities = int(utilities_list[0])

                if available_date:
                    available_date = (available_date.lower()).replace("frei ab", "")
                    if "sofort" in available_date:
                        available_date = None
                    elif "januar" in available_date.lower():
                        available_date = "2022-01-01"
                    elif "." in available_date:
                        available_date = available_date.split(".")
                        day = available_date[0]
                        month = available_date[1]
                        year = available_date[2]
                        available_date = year.strip() + "-" + month.strip() + "-" + day.strip()

            landlord_info = response.xpath('.//div[contains(@class, "ansprech")]')
            landlord_name = landlord_info.xpath('.//strong/text()').extract()
            landlord_number = landlord_info.xpath('.//div[contains(@class, "text")]/text()')[0].extract()
            landlord_number = landlord_number.replace("Durchwahl:", "")
            landlord_number = landlord_number.replace("Telefon", "")
            landlord_number = landlord_number.strip()
            landlord_email = "info@bruening-immo.de"

            images_all = response.xpath('.//div[contains(@class, "bildzahlen")]//a/@style').extract()
            images = []
            for image in images_all:
                image = image.split("..")
                image = image[1]
                image = image.split(")")
                image = image[0]
                images.append("https://bruening-immo.de/" + image)

            address = title[0].split()
            address = address[address.index("in"):]
            if address[0] == "in":
                del address[0]
            if "in" in address:
                address[address.index("in")] = ","
            address = (" ".join(address)).strip() + ", Germany"
            long, lat = extract_location_from_address(address)
            longitude = str(long)
            latitude = str(lat)
            zipcode, city, no_address = extract_location_from_coordinates(longitude, latitude)
            if zipcode == "":
                zipcode = None

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
            # item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            #item_loader.add_value("washing_machine", washing_machine) # Boolean
            #item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            item_loader.add_value("deposit", deposit) # Int
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
