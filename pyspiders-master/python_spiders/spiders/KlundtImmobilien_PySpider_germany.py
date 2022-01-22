# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_location_from_address


class KlundtimmobilienPyspiderGermanySpider(scrapy.Spider):
    name = "KlundtImmobilien"
    start_urls = ['https://klundt-immobilien.de/mietobjekte-kaufobjekte/']
    allowed_domains = ["klundt-immobilien.de", "portal.immobilienscout24.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_link)

    def parse_link(self, response):
        url = response.xpath('.//iframe[contains(@name, "immoscout24")]/@src')[0].extract()
        yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        apartments_divs = response.xpath('.//ul[contains(@class, "result--list")]//li')
        for apartment_div in apartments_divs:
            rent_type = apartment_div.xpath('.//div[contains(@class, "result__list__element--infos")]//p/text()').extract()
            rent_type_final = []
            if rent_type:
                for item in rent_type:
                    item = item.strip()
                    rent_type_final.append(item)
            if "Wohnung zur Miete" in rent_type_final:
                apartment_url = apartment_div.xpath('.//figure//a/@href').extract()
                url = "https://portal.immobilienscout24.de" + apartment_url[0]
                apartment_title = \
                apartment_div.xpath('.//div[contains(@class, "result__list__element--infos")]//h3//a/text()')[
                    0].extract()
                apartment_balcony = apartment_div.xpath(
                    './/div[contains(@class, "result__list__element--infos")]//h3//em[contains(@class, "is24portale-tag19")]/text()').extract()
                address = apartment_div.xpath(
                    './/div[contains(@class, "result__list__element--infos")]//div[contains(@class, "result__list__element__infos--location")]//p/text()')[0].extract()
                rent = apartment_div.xpath('.//div[contains(@class, "result__list__element--infos")]//ul//li[1]//span/text()')[0].extract()
                square_meters = apartment_div.xpath('.//div[contains(@class, "result__list__element--infos")]//ul//li[2]//span/text()')[0].extract()
                room_count = apartment_div.xpath('.//div[contains(@class, "result__list__element--infos")]//ul//li[3]//span/text()')[0].extract()
                yield scrapy.Request(url, callback=self.populate_item, meta={
                    "title": apartment_title,
                    "balcony": apartment_balcony,
                    "address": address,
                    "rent": rent,
                    "square_meters": square_meters,
                    "room_count": room_count,
                })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.meta.get("title")

        balcony_exist = response.meta.get("balcony")
        balcony = None
        if len(balcony_exist) > 0:
            balcony = True

        address = response.meta.get("address")
        address = address + ", Germany"
        longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)
        zipcode, city, no_address = extract_location_from_coordinates(longitude, latitude)

        rent = response.meta.get("rent")
        rent = rent.replace("€", "")
        rent = rent.replace(".", "")
        rent = rent.replace(",", ".")
        rent = float(rent)
        rent = round(rent)
        rent = int(rent)

        square_meters = response.meta.get("square_meters")
        square_meters = (square_meters.split())[0]
        square_meters = square_meters.replace(".", "")
        square_meters = square_meters.replace(",", ".")
        square_meters = float(square_meters)
        square_meters = round(square_meters)
        square_meters = int(square_meters)

        room_count = response.meta.get("room_count")
        room_count = room_count.replace(",", ".")
        room_count = float(room_count)
        room_count = round(room_count)
        room_count = int(room_count)

        images_raw_url = response.xpath('.//div[contains(@class, "sp-slide")]//div//a/@href').extract()
        images = []
        for image in images_raw_url:
            image = "https:" + image
            images.append(image)

        apartment_info = response.xpath('.//div[contains(@class, "expose--text")]')
        utilities = None
        terrace = None
        bathroom_count = None
        energy_label = None
        available_date = None
        deposit = None
        parking = None
        description = None
        floor_plan_images = []
        for apartment_single_info in apartment_info:
            div_title = apartment_single_info.xpath('.//h4/text()').extract()
            if "Weitere Daten" in div_title:
                current_div = apartment_single_info.xpath('.//ul//li')
                for item in current_div:
                    rooms_energy = item.xpath('.//p[1]/text()').extract()
                    if "Balkon/ Terrasse:" in rooms_energy:
                        balcony = True
                        terrace = True
                    elif "Badezimmer:" in rooms_energy:
                        bathroom_count = item.xpath('.//p[2]/text()').extract()
                    elif "Energieeffizienzklasse:" in rooms_energy:
                        energy_label = item.xpath('.//p[2]/text()').extract()
                    elif "Bezugsfrei ab:" in rooms_energy:
                        available_date = item.xpath('.//p[2]/text()')[0].extract()
                        if "sofort" in available_date:
                            available_date = None
                        else:
                            available_date = available_date.split(".")
                            day = available_date[0]
                            month = available_date[1]
                            year = available_date[2]
                            available_date = year + "-" + month + "-" + day
            elif "Kosten" in div_title:
                current_div = apartment_single_info.xpath('.//ul//li')
                for item in current_div:
                    costs = item.xpath('.//p[1]/text()').extract()
                    if "Nebenkosten:" in costs:
                        utilities = item.xpath('.//p[2]/text()')[0].extract()
                        utilities = utilities.replace("+", "")
                        utilities = utilities.replace("EUR", "")
                        utilities = utilities.replace(".", "")
                        utilities = utilities.replace(",", ".")
                        utilities = float(utilities)
                        utilities = round(utilities)
                        utilities = int(utilities)
                    elif "Kaution" in costs[0]:
                        deposit = item.xpath('.//p[2]/text()')[0].extract()
                        deposit = deposit.split("(")
                        deposit = deposit[1]
                        deposit = deposit.replace("x", "")
                        deposit = deposit.split()
                        deposit = deposit[0]
                        deposit = int(deposit) * rent
            elif "Objektbeschreibung" in div_title:
                description = apartment_single_info.xpath('.//p/text()').extract()
            elif "Ausstattung" in div_title:
                furnishing_info = apartment_single_info.xpath('.//p/text()').extract()
                for item in furnishing_info:
                    if "Stellplätze" in item:
                        parking = True
                    if "Kein Stellplatz" in item:
                        parking = False
                    elif "Garage" in item:
                        parking = True
                    elif "Einzelgarage " in item:
                        parking = True
            elif "Grundriss" in div_title:
                floor_plan_images_raw = apartment_single_info.xpath('.//img/@src').extract()
                for image in floor_plan_images_raw:
                    floor_plan_images.append("https:" + image)

        property_type = "apartment"

        landlord_name = "Klundt Immobilien - Andreas Klundt"
        landlord_number = "0821 8104996"
        landlord_email = "info@klundt-immobilien.de"

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        # item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", latitude)  # String
        item_loader.add_value("longitude", longitude)  # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type",
                              property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available_date)  # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        item_loader.add_value("deposit", deposit)  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1
        yield item_loader.load_item()
