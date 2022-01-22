# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address, extract_location_from_coordinates


class EssingerwohnungsbauPyspiderGermanySpider(scrapy.Spider):
    name = "Essingerwohnungsbau"
    start_urls = ['http://essingerwohnungsbau.de/-Wohnen/-Miete/']
    allowed_domains = ["essingerwohnungsbau.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, headers={"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36", "Accept": "*/*", "Accept-Encoding": "gzip, deflate, br"})

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        # Your code goes here
        apartments_divs = response.xpath('.//div[contains(@id , "MUNIT-55768ed9-f594-44be-8f45-551ff7e6c961-MUNIT")]//div[contains(@class, "rz_grid")]//div//div')
        for apartment_div in apartments_divs:
            apartment_url = apartment_div.xpath('.//a/@href').extract()
            if apartment_url:
                if (apartment_url[0]).count("/") > 2:
                    apartment_url = "http://essingerwohnungsbau.de" + apartment_url[0]
                    apartment_title = apartment_div.xpath('.//a//div[contains(@class, "text")]//p/text()').extract()
                    apartment_title = apartment_title + apartment_div.xpath('.//a//div[contains(@class, "text")]//span/text()').extract()
                    if not apartment_title:
                        apartment_title = apartment_div.xpath('.//a//div[contains(@class, "text")]//p//span/text()').extract()
                    yield scrapy.Request(url=apartment_url, callback=self.populate_item, meta={"apartment_title": apartment_title})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        apartment_title = response.meta.get("apartment_title")
        apartment_title = "".join(apartment_title)
        apartment_title = apartment_title.split("-")
        title = apartment_title[0]
        room_count_all = []
        square_meters_all = []
        room_count = apartment_title[1]
        square_meters = apartment_title[3]
        for item in room_count:
            if item.isdigit():
                room_count_all.append(item)
        for item in square_meters:
            if item.isdigit():
                square_meters_all.append(item)

        for unit in room_count_all:
            rent_list = []
            utilities_list = []
            square_meters_both = []
            energy_label = None
            deposit = None
            rent = None
            utilities = None
            deposit_unit = None
            item_loader = ListingLoader(response=response)
            room_count = int(unit)
            description_1 = response.xpath('.//div[contains(@class, "text")]//p/text()').extract()
            description_2 = response.xpath('.//div[contains(@class, "text")]//p//span/text()').extract()
            whole_description = description_1 + description_2
            for item in whole_description:
                if "energieausweis" in item.lower():
                    energy_label = (item.split(","))[-1]
                elif item.startswith("Kaltmiete"):
                    rent_all = (item.split(":"))[1]
                    rent_all = rent_all.replace(",", " ")
                    rent_all = rent_all.replace(".", "")
                    for rent_num in rent_all.split():
                        if rent_num.isdigit():
                            rent_list.append(rent_num)
                elif "kaution" in item.lower():
                    deposit_all = (item.split(":"))[1]
                    deposit_unit = (deposit_all.split())[0]
                elif item.lower().startswith("nebenkostenvorauszahlung"):
                    utilities_all = (item.split(":"))[1]
                    utilities_all = utilities_all.replace(",-", " ")
                    for utilities_num in utilities_all.split():
                        if utilities_num.isdigit():
                            utilities_list.append(utilities_num)
                elif "48 - 87" in item:
                    square_meters_both = (item.split(","))[1]
                    square_meters_both = (square_meters_both.split("-"))

            external_link = None
            if len(rent_list) > 1:
                if room_count == 2:
                    external_link = response.url + "#2-room-apartment"
                    rent = int(rent_list[0])
                    utilities = int(utilities_list[0])
                    square_meters = int(square_meters_both[0].strip())
                    deposit = int(deposit_unit) * rent
                elif room_count == 3:
                    external_link = response.url + "#3-room-apartment"
                    rent = int(rent_list[1])
                    utilities = int(utilities_list[1])
                    square_meters = int(square_meters_both[1].strip())
                    deposit = int(deposit_unit) * rent
            else:
                external_link = response.url
                rent = int(rent_list[0])
                utilities = int(utilities_list[0])
                square_meters = int((square_meters.split())[0])
                deposit = int(deposit_unit) * rent

            images_all = response.xpath('.//div[contains(@class, "hasImage")]//img/@srcset').extract()
            if not images_all:
                images_all = response.xpath('.//div[contains(@class, "hasImage")]//img/@data-srcset').extract()
            images = []
            for image in images_all:
                image = (image.split(","))[0]
                image = image.replace("320w", "")
                image = image.replace("480w", "")
                images.append("http://essingerwohnungsbau.de" + image)

            landlord_number = "07365 9603-33"
            landlord_name = "Essinger Wohnungsbau GmbH"

            description = None
            for item in whole_description:
                if len(item) > 200:
                    description = item

            address = title + ", Germany"
            long, lat = extract_location_from_address(address)
            longitude = str(long)
            latitude = str(lat)
            zipcode, city, no_address = extract_location_from_coordinates(longitude, latitude)

            # # MetaData
            item_loader.add_value("external_link", external_link) # String
            item_loader.add_value("external_source", self.external_source) # String

            #item_loader.add_value("external_id", external_id) # String
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
            item_loader.add_value("property_type", "apartment") # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            #item_loader.add_value("bathroom_count", bathroom_count) # Int

            #item_loader.add_value("available_date", available_date) # String => date_format

            #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            #item_loader.add_value("parking", parking) # Boolean
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
            #item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
