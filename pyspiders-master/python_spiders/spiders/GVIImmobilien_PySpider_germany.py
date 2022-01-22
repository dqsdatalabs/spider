# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address


class GviimmobilienPyspiderGermanyDeSpider(scrapy.Spider):
    name = "GVIImmobilien"
    start_urls = ['https://www.das-berater.team/Mietangebote.htm']
    allowed_domains = ["das-berater.team"]
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

    def parse(self, response, **kwargs):
        pages_url = response.xpath('.//div[contains(@class, "blaetternavigation")]//li[contains(@class, "seite")]//a/@href').extract()
        for url in pages_url:
            url = url.replace("/", "")
            url = "https://www.das-berater.team/" + url
            yield scrapy.Request(url, callback=self.parse_pages, dont_filter=True)

    # 2. SCRAPING level 2
    def parse_pages(self, response):
        apartments_divs = response.xpath('.//div[contains(@class, "col-lg-6")]')
        for apartment_div in apartments_divs:
            apartment_url = apartment_div.xpath('.//a[contains(@class, "hover_effekt")]/@href').extract()
            if len(apartment_url) >= 1:
                availability = apartment_div.xpath('.//div[contains(@class, "bg_image")]//img/@alt')[0].extract()
                if not availability.startswith("reserviert"):
                    if not availability.startswith("vermietet"):
                        url = "https://www.das-berater.team" + apartment_url[0]
                        rent = apartment_div.xpath('.//a[contains(@class, "hover_effekt")]//div[contains(@class, "preisposition")]//div[contains(@class, "preis")]//span/text()').extract()
                        city = apartment_div.xpath('.//div[contains(@class, "hauptinfos")]//div[contains(@class, "ort")]/text()').extract()
                        property_type = apartment_div.xpath('.//div[contains(@class, "hauptinfos")]//div[contains(@class, "objektart")]/text()').extract()
                        title = apartment_div.xpath('.//div[contains(@class, "hauptinfos")]//h3//a/text()').extract()
                        property_info = apartment_div.xpath('.//div[contains(@class, "objektinfos")]//div[contains(@class, "info")]//b/text()').extract()
                        yield scrapy.Request(url, callback=self.populate_item, meta={
                            "rent": rent,
                            "city": city,
                            "property_type": property_type,
                            "title": title,
                            "property_info": property_info,
                        })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rent = response.meta.get("rent")
        rent = rent[0]
        rent = rent.split("-")[0]
        rent = rent.replace(".", "")
        rent = rent.replace(",", ".")
        rent = float(rent)
        rent = round(rent)
        rent = int(rent)

        city = response.meta.get("city")

        property_type = response.meta.get("property_type")
        if property_type[0] == "Wohnung":
            property_type = "apartment"

        title = response.meta.get("title")

        property_info = response.meta.get("property_info")

        square_meters = property_info[0]
        square_meters = (square_meters.split())[0]
        square_meters = square_meters.replace(".", "")
        square_meters = square_meters.replace(",", ".")
        square_meters = float(square_meters)
        square_meters = round(square_meters)
        square_meters = int(square_meters)

        external_id = property_info[-1]

        room_count = None
        if len(property_info) == 3:
            room_count = property_info[1]
            room_count = room_count.replace(",", ".")
            room_count = float(room_count)
            room_count = round(room_count)
            room_count = int(room_count)

        if not room_count:
            title_info = title[0]
            title_info = title_info.split("|")
            for item in title_info:
                if "ZIMMER" in item:
                    room_count = item.split("-ZIMMER")[0]
                    room_count = room_count.strip()
                    room_count = room_count.replace(",", ".")
                    room_count = float(room_count)
                    room_count = round(room_count)
                    room_count = int(room_count)

        images_list = response.xpath('.//div[contains(@id, "objektbildcarousel")]//div//img/@src').extract()
        images = []
        for image in images_list:
            image = "https://www.das-berater.team/" + image
            images.append(image)

        address = response.xpath('.//div[contains(@class, "bg")]//div[contains(@class, "pd")]//div/text()')[1].extract()

        room_info_keys = response.xpath('.//div[contains(@class, "bg")]//div[contains(@class, "pd")]//div[contains(@class, "row")]//div//span[contains(@class, "key")]/text()')[1:].extract()
        room_info_values = response.xpath('.//div[contains(@class, "bg")]//div[contains(@class, "pd")]//div[contains(@class, "row")]//div//span[contains(@class, "wert")]/text()').extract()
        room_info = dict(zip(room_info_keys, room_info_values))
        if "Zimmer" in room_info.keys():
            room_count = room_info["Zimmer"]
            room_count = room_count.replace(",", ".")
            room_count = float(room_count)
            room_count = round(room_count)
            room_count = int(room_count)

        apartment_info_divs = response.xpath('.//div[contains(@class, "weiteredaten")]//div[contains(@class, "col-lg-6")]//div')
        apartment_info_values = []
        apartment_info_keys = []
        for div in apartment_info_divs:
            if (div.extract()).count("col-pt-6") > 1:
                apartment_info_key = div.xpath('.//div//span[contains(@class, "key")]/text()').extract()
                apartment_info_value = div.xpath('.//div//span[contains(@class, "wert")]/text()').extract()
                if not apartment_info_value:
                    apartment_info_value = div.xpath('.//i/@class').extract()
                apartment_info_values.append(apartment_info_value[0])
                apartment_info_keys.append(apartment_info_key[0])
        apartment_info = dict(zip(apartment_info_keys, apartment_info_values))
        parking = None
        elevator = None
        utilities = None
        deposit = None
        balcony = None
        if "Stellplätze" in apartment_info.keys():
            parking = True
        if "Aufzug" in apartment_info.keys():
            elevator = True
        if "Nebenkosten (ca.)" in apartment_info.keys():
            utilities = apartment_info["Nebenkosten (ca.)"]
            utilities = utilities.split("-")[0]
            utilities = utilities.split(" ")[0]
            utilities = utilities.replace(".", "")
            utilities = utilities.replace(",", ".")
            utilities = float(utilities)
            utilities = round(utilities)
            utilities = int(utilities)
        if "Kaution" in apartment_info.keys():
            deposit = apartment_info["Kaution"]
            deposit = deposit.split("-")[0]
            deposit = deposit.replace(".", "")
            deposit = deposit.replace(",", ".")
            deposit = float(deposit)
            deposit = round(deposit)
            deposit = int(deposit)

        description = response.xpath('.//div[contains(@class, "beschreibung")]//p/text()').extract()

        landlord_name = response.css('div.col-tb-12.col-lg-12 div::text').extract()
        landlord_number_exist = response.css('div.col-tb-12.col-lg-12::text').extract()
        landlord_number = None
        for num in landlord_number_exist:
            if len(num) > 5:
                landlord_number = num
                landlord_number = landlord_number.split(":")[1]
                landlord_number = landlord_number.strip()
        landlord_email = response.css('div.col-tb-12.col-lg-12 a::text').extract()

        amenities = response.xpath('.//div[contains(@class, "textausstattung ")]//p/text()').extract()
        if "Balkon" in amenities:
            balcony = True

        energy = response.xpath('.//div[contains(@class, "sonstiges")]//p/text()').extract()
        energy_label = None
        for item in energy:
            if "Energieeffizienzklasse" in item:
                energy_label = item.split("Energieeffizienzklasse:")[1]
                energy_label = energy_label.split()[0]

        zipcode = address.split(city[0])[0]
        zipcode = zipcode.strip()
        longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
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
