# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader


class EgwohnenPyspiderGermanySpider(scrapy.Spider):
    name = "eGWohnen"
    start_urls = [
        'https://www.eg-wohnen.de/de/wohnen/wohnungsangebote.php',
        'https://www.eg-wohnen.de/de/wohnen/wohnungsangebote.php?pageId5bdbbc56=2#list_5bdbbc56'
    ]
    allowed_domains = ["eg-wohnen.de"]
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
        apartments_divs = response.xpath('.//li[contains(@class, "listEntry")]')
        for apartment_div in apartments_divs:
            title = apartment_div.xpath('.//h3[contains(@class, "listEntryTitle")]//a/text()').extract()
            apartment_url = apartment_div.xpath('.//h3[contains(@class, "listEntryTitle")]//a/@href').extract()
            url = "https://www.eg-wohnen.de" + apartment_url[0]
            yield scrapy.Request(url, callback=self.populate_item, meta={"title": title})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.meta.get("title")

        apartment_table = response.xpath('.//div[contains(@id, "container_dbb2212469596ade84abb9069d6a2e3d_1")]//div[contains(@class, "myFloat5050")]')
        apartment_table_keys = apartment_table.xpath('.//div[1]//text()').extract()
        apartment_table_values = apartment_table.xpath('.//div[2]//text()').extract()
        apartment_dict = dict(zip(apartment_table_keys, apartment_table_values))

        external_id = apartment_dict["Objekt-Nr."]

        address = apartment_dict["Straße, Nr."]
        zipcode_city = apartment_dict["PLZ / Ort"]
        zipcode_city_new = zipcode_city.split()
        zipcode = zipcode_city_new[0]
        city = zipcode_city_new[1]
        address = address + ", " + zipcode_city

        room_count = apartment_dict["Zimmerzahl"]
        room_count = int(room_count)

        square_meters = apartment_dict["Wohnfläche"]
        square_meters = (square_meters.split())[0]
        square_meters = float(square_meters)
        square_meters = round(square_meters)
        square_meters = int(square_meters)

        floor = apartment_dict["Etage"]

        elevator_exist = apartment_dict["Fahrstuhl"]
        elevator = None
        if elevator_exist != "Nein":
            elevator = True

        rent = apartment_dict["Nutzungsgebühr"]
        rent = (rent.split())[0]
        rent = rent.replace(".", "")
        rent = rent.replace(",", ".")
        rent = float(rent)
        rent = round(rent)
        rent = int(rent)

        utilities = apartment_dict["Betriebskosten"]
        utilities = (utilities.split())[0]
        utilities = utilities.replace(".", "")
        utilities = utilities.replace(",", ".")
        utilities = float(utilities)
        utilities = round(utilities)
        utilities = int(utilities)

        available_date = apartment_dict["Bezugsfrei ab"]
        if available_date == "sofort":
            available_date = None
        else:
            available_date = available_date.split(".")
            day = available_date[0]
            month = available_date[1]
            year = available_date[2]
            available_date = year.strip() + "-" + month.strip() + "-" + day.strip()

        amenities = response.xpath('.//div[contains(@id, "container_dbb2212469596ade84abb9069d6a2e3d_1")]//ul//li/text()').extract()
        amenities = " ".join(amenities)
        balcony = None
        if "balkon" in amenities:
            balcony = True
        description = amenities
        description = description.replace("\n", "")

        amenities_two = response.xpath('.//div[contains(@id, "container_dbb2212469596ade84abb9069d6a2e3d_3")]//ul//li/text()').extract()
        amenities_two = " ". join(amenities_two)
        parking = None
        if "PKW-Stellplätze" in amenities_two:
            parking = True

        images_all = response.xpath('.//div[contains(@class, "col2")]//img[1]/@src').extract()
        images = []
        floor_plan_images = []
        images.append(images_all[0])
        floor_plan_images.append("https://www.eg-wohnen.de" + images_all[1])

        landlord_name = 'eG Wohnen 1902 VermietungsCen'
        landlord_number = '0355 7528-350'
        landlord_email = 'vermietungscenter@eg-wohnen.de'

        long_lat = response.xpath('.//script/text()').extract()
        long_lat_script = ""
        for item in long_lat:
            if "initMap" in item:
                long_lat_script = item
        long_lat_script = long_lat_script.split("longtitude")[1]
        long_lat_script = long_lat_script.split("latLng")[0]
        long_lat_script = long_lat_script.split("(Number")
        longitude = long_lat_script[1].strip()
        longitude = longitude.split("'")[1]
        latitude = long_lat_script[2].strip()
        latitude = latitude.split("'")[1]

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
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
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
