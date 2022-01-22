# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_location_from_address


class WalthersohnimmobilienPyspiderGermanySpider(scrapy.Spider):
    name = "WaltherSohnImmobilien"
    start_urls = ['https://www.walther-sohn.de/vermietung?list%5Bfullordering%5D=immobilieid+DESC&list%5Blimit%5D=0&limit=&6c62fc0514883ff5cbe250f4d3d582b1=1']
    allowed_domains = ["walther-sohn.de"]
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
        apartments_divs = response.xpath('.//div[contains(@class, "well-firmennest")]')
        for apartment_div in apartments_divs:
            apartment_info = apartment_div.xpath('.//div[contains(@class, "bsdimmo-infoblock")]')
            apartment_info_keys = apartment_info.xpath('.//div[contains(@class, "objektdaten")]//div//label/text()').extract()
            apartment_keys = []
            for key in apartment_info_keys:
                key = key.strip()
                apartment_keys.append(key)
            apartment_info_values = apartment_info.xpath('.//div[contains(@class, "objektdaten")]//div//p/text()').extract()
            apartment_values = []
            for value in apartment_info_values:
                value = value.strip()
                apartment_values.append(value)
            apartment_info_dict = dict(zip(apartment_keys, apartment_values))
            if "Wohnfläche ca.:" in apartment_info_dict.keys():
                title = apartment_div.xpath('.//h4//a/text()').extract()
                apartment_url = apartment_div.xpath('.//h4//a/@href')[0].extract()
                url = "https://www.walther-sohn.de" + apartment_url
                yield scrapy.Request(url, callback=self.populate_item, meta={
                    "title": title,
                    "apartment_info_dict": apartment_info_dict
                })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.meta.get("title")

        apartment_info_dict = response.meta.get("apartment_info_dict")

        rent = apartment_info_dict["Kaltmiete:"]
        rent = (rent.split())[0]
        rent = rent.replace(".", "")
        rent = rent.replace(",", ".")
        rent = float(rent)
        rent = round(rent)
        rent = int(rent)


        square_meters = apartment_info_dict["Wohnfläche ca.:"]
        square_meters = (square_meters.split())[0]
        square_meters = square_meters.replace(".", "")
        square_meters = square_meters.replace(",", ".")
        square_meters = float(square_meters)
        square_meters = round(square_meters)
        square_meters = int(square_meters)

        room_count = apartment_info_dict["Zimmer:"]
        room_count = room_count.split()[0]
        room_count = int(room_count)

        floor = apartment_info_dict["Etage:"]
        floor = floor.replace(" ", "")

        if "" in apartment_info_dict.keys():
            address = apartment_info_dict["Ort:"] + apartment_info_dict[""]
        else:
            address = apartment_info_dict["Ort:"]

        property_type = "apartment"

        images = response.xpath('.//div[contains(@class, "fotorama")]//a/@href').extract()

        external_id = response.xpath('.//div[contains(@id, "hauptinfo")]//span/text()')[0].extract()
        external_id = external_id.replace("Objekt ID:", "")
        external_id = external_id.strip()

        prices_info_keys = response.xpath('.//div[contains(@class, "bsdimmo-preise")]//div[contains(@class, "row")]//div[contains(@class, "name")]/text()').extract()
        prices_info_values = response.xpath('.//div[contains(@class, "bsdimmo-preise")]//div[contains(@class, "row")]//div[contains(@class, "value")]/text()').extract()
        prices_info_dict = dict(zip(prices_info_keys, prices_info_values))

        warm_rent = prices_info_dict["Warmmiete: "]
        warm_rent = (warm_rent.split())[0]
        warm_rent = warm_rent.replace(".", "")
        warm_rent = warm_rent.replace(",", ".")
        warm_rent = float(warm_rent)
        warm_rent = round(warm_rent)
        warm_rent = int(warm_rent)

        utilities = warm_rent - rent

        deposit = prices_info_dict["Kaution: "]
        deposit = (deposit.split())[0]
        deposit = deposit.replace(".", "")
        deposit = deposit.replace(",", ".")
        deposit = float(deposit)
        deposit = round(deposit)
        deposit = int(deposit)

        amenities = response.xpath('.//div[contains(@class, "bsdimmo-ausstattung")]//div[contains(@class, "row")]//div[contains(@class, "name")]/text()').extract()
        elevator = None
        terrace = None
        balcony = None
        parking = None
        bathroom_count = None
        if "Personenaufzug" in amenities:
            elevator = True

        rooms_keys = response.xpath('.//div[contains(@class, "bsdimmo-flaechen")]//div[contains(@class, "row")]//div[contains(@class, "name")]/text()').extract()
        rooms_values = response.xpath('.//div[contains(@class, "bsdimmo-flaechen")]//div[contains(@class, "row")]//div[contains(@class, "value")]/text()').extract()
        rooms = dict(zip(rooms_keys, rooms_values))
        if "Badezimmer: " in rooms.keys():
            bathroom_count = rooms["Badezimmer: "]
            bathroom_count = int(bathroom_count)
        if "Terrassen: " in rooms.keys():
            terrace_exist = rooms["Terrassen: "]
            if terrace_exist:
                terrace = True
        if "Balkone: " in rooms.keys():
            balcony_exist = rooms["Balkone: "]
            if balcony_exist:
                balcony = True

        available_date = response.xpath('.//div[contains(@class, "bsdimmo-objekt")]//div[contains(@class, "row")]//div[contains(@class, "value")]/text()')[0].extract()
        if "sofort" in available_date:
            available_date = None
        else:
            available_date = available_date.split(".")
            day = available_date[0]
            month = available_date[1]
            year = available_date[2]
            available_date = year.strip() + "-" + month.strip() + "-" + day.strip()

        description = response.xpath('.//div[contains(@class, "accordion-content")]//div/text()')[:2].extract()
        description = " ".join(description)
        if "Haftungsausschluss" in description:
            description = description[:description.index('Haftungsausschluss')]

        if "Garage" in description:
            parking = True

        landlord_info = response.xpath('.//div[contains(@class, "bsdimmo-makler")]//div[contains(@class, "row")]//div//div/text()').extract()
        landlord_number = landlord_info[-2]
        landlord_name = landlord_info[:len(landlord_info)-2]
        landlord_name = " ".join(landlord_name)
        landlord_name = landlord_name.replace("\t", "")
        landlord_name = landlord_name.replace("\r", "")
        landlord_name = landlord_name.replace("\n", "")
        landlord_email = "vermietung@walther-sohn.de"

        address = address + ", Germany"
        longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)
        zipcode, city, no_address = extract_location_from_coordinates(longitude, latitude)

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
        # item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
