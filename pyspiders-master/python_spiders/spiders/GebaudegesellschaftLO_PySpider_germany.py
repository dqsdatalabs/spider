# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_location_from_address


class GebaudegesellschaftloPyspiderGermanySpider(scrapy.Spider):
    name = "GebaudegesellschaftLO"
    start_urls = ['https://www.glo-online.de/freie-wohnungen-in-limbach-oberfrohna/']
    allowed_domains = ["glo-online.de"]
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
        apartments_divs = response.xpath('.//div[contains(@class, "apartments")]//div[contains(@class, "item")]')
        for apartment_div in apartments_divs:
            external_id = apartment_div.xpath('./@class').extract()
            apartment_url = apartment_div.xpath('.//a/@href').extract()
            for item in apartment_url:
                if "/" in item:
                    url = "https://www.glo-online.de" + item
            title = apartment_div.xpath('.//h3[contains(@class, "item-title")]/text()').extract()
            address = apartment_div.xpath('.//h3[contains(@class, "item-address")]/text()').extract()
            details_keys = apartment_div.xpath('.//div[contains(@class, "item-details")]//dt/text()').extract()
            details_values = apartment_div.xpath('.//div[contains(@class, "item-details")]//dd/text()').extract()
            details = dict(zip(details_keys, details_values))
            yield scrapy.Request(url, callback=self.populate_item, meta={
                "external_id": external_id,
                "title": title,
                "address": address,
                "details": details,
            })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.meta.get("title")

        details = response.meta.get("details")
        room_count = details["Zimmer"]
        room_count = float(room_count)
        room_count = round(room_count)
        room_count = int(room_count)

        square_meters = details["Fläche"]
        square_meters = square_meters.replace("ca.", "")
        square_meters = square_meters.replace("m", "")
        square_meters = float(square_meters)
        square_meters = round(square_meters)
        square_meters = int(square_meters)

        floor = details["Lage"]
        floor = floor.replace("G", "")
        floor = floor.replace(".O", "")
        if floor == "E":
            floor = "Erdgeschoss"

        apartment_details = response.xpath('.//div[contains(@class, "apartment-details")]//dl')
        apartment_details_keys = apartment_details.xpath('.//dt/text()').extract()
        apartment_details_values = apartment_details.xpath('.//dd/text()').extract()
        apartment_details_dict = dict(zip(apartment_details_keys, apartment_details_values))

        rent = apartment_details_dict["Grundmiete"]
        rent = rent.replace("€", "")
        rent = rent.replace(".", "")
        rent = rent.replace(",", ".")
        rent = float(rent)
        rent = round(rent)
        rent = int(rent)

        utilities = apartment_details_dict["Nebenkosten"]
        utilities = utilities.replace("€", "")
        utilities = utilities.replace(".", "")
        utilities = utilities.replace(",", ".")
        utilities = float(utilities)
        utilities = round(utilities)
        utilities = int(utilities)

        deposit = apartment_details_dict["Kaution"]
        deposit = deposit.replace("€", "")
        deposit = deposit.replace(".", "")
        deposit = deposit.replace(",", ".")
        deposit = float(deposit)
        deposit = round(deposit)
        deposit = int(deposit)

        available_date = apartment_details_dict["Frei ab"]
        available_date = available_date.strip()
        if "sofort" in available_date:
            available_date = None
        else:
            available_date = available_date.split(".")
            day = available_date[0]
            month = available_date[1]
            year = available_date[2]
            available_date = year + "-" + month + "-" + day

        images = response.xpath('.//div[contains(@class, "apartment-images")]//a/@href').extract()

        apartment_data = response.xpath('.//div[contains(@class, "apartment-data")]//small/text()').extract()
        apartment_data_new = []
        for item in apartment_data:
            apartment_data_new.append(item.strip())

        external_id = apartment_data_new[0]
        external_id = external_id.split(":")
        external_id = external_id[1]
        external_id = external_id.strip()

        address = apartment_data_new[1]

        description = response.xpath('normalize-space(.//div[contains(@class, "content")]//p[1]/text())').extract()

        furnishing_data = response.xpath('.//ul[contains(@class, "environmet-list")]//li/text()').extract()
        parking = None
        balcony = None
        elevator = None
        for item in furnishing_data:
            if "Stellplatz" in item:
                parking = True
            if "Balkon" in item:
                balcony = True
            if "Fahrstuhl" in item:
                elevator = True

        property_type = "apartment"

        landlord_data = response.xpath('.//div[contains(@class, "contact-person")]//div')
        landlord_name = landlord_data.xpath('.//h4/text()').extract()
        landlord_number = landlord_data.xpath('.//p/text()')[0].extract()
        landlord_number = (landlord_number.split(":"))[1]
        landlord_email = landlord_data.xpath('.//p//a/text()').extract()

        longitude_latitude = response.xpath('//script/text()').extract()
        longitude = None
        latitude = None
        for item in longitude_latitude:
            if "map" in item:
                item = (item.split("var latitude"))[1]
                longitude_latitude_final = (item.split("map"))[0]
                longitude_latitude_final = longitude_latitude_final.strip()
                longitude_latitude_final = longitude_latitude_final.replace("var", "")
                longitude_latitude_final = longitude_latitude_final.split("longitude")
                latitude = longitude_latitude_final[0]
                latitude = latitude.replace("=", "")
                latitude = latitude.replace(";", "")
                latitude = latitude.strip()
                longitude = longitude_latitude_final[1]
                longitude = longitude.replace("=", "")
                longitude = longitude.replace(";", "")
                longitude = longitude.strip()
        if longitude == "":
            longitude = None
            latitude = None

        if longitude:
            zipcode, city, no_address = extract_location_from_coordinates(longitude, latitude)
        else:
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
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

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

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
