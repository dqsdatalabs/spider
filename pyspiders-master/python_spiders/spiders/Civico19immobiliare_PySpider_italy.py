# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
from ..helper import extract_location_from_address, extract_location_from_coordinates


class Civico19immobiliarePyspiderItalySpider(scrapy.Spider):
    name = "civico19immobiliare"
    start_urls = ['https://www.civico19immobiliare.it/ita/immobili?order_by=&seo=&rental=1&property_type_id=1&city_id=&price_max=&size_min=&size_max=']
    allowed_domains = ["civico19immobiliare.it"]
    country = 'italy'  # Fill in the Country's name
    locale = 'it'  # Fill in the Country's locale, look up the docs if unsure
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
        # Your code goes here
        landlord_phone = response.xpath('.//div[contains(@class, "contacts")]//span[contains(@class, "tel")]/text()')[0].extract()
        landlord_email = response.xpath('.//div[contains(@class, "contacts")]//span[contains(@class, "email")]//a/text()')[0].extract()
        all_apartments_div = response.xpath('.//div[contains(@id, "main_content")]//div[contains(@id, "immobili_elenco")]')
        all_apartments_div_sep = all_apartments_div.xpath('.//div[contains(@class, "property")]')
        for apartment_div in all_apartments_div_sep:
            rent = apartment_div.xpath('.//div[contains(@class, "preview")]//li[contains(@class, "price")]/text()').extract()
            apartment_link = apartment_div.xpath('.//a/@href').extract()[0]
            if not rent:
                continue
            yield Request(url=apartment_link, callback=self.populate_item, meta={"rent": rent, "landlord_phone": landlord_phone, "landlord_email": landlord_email})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        landlord_phone = response.meta.get("landlord_phone")
        landlord_email = response.meta.get("landlord_email")
        apartment_info = response.xpath('.//div[contains(@id, "main_content")]')
        listsplitted = str(apartment_info.xpath('normalize-space(string(.//h2[contains(@class, "title")]))')
                           .extract()[0]).split('Appartamento, ')
        external_id = listsplitted[0].replace("Rif.", "")
        title = 'Appartamento, ' + str(listsplitted[1:][0])
        description = apartment_info.xpath('normalize-space(string(.//div[contains(@id, "tab-description")]))').extract()
        locationOfMaggiori = str(description).upper().index("MAGGIORI")
        description = str(description)[:locationOfMaggiori]
        available_date_existense = description.count("Disponibile da")
        available_date = None
        if available_date_existense > 0:
            available_date_substring = description[description.index("Disponibile da"):]
            available_date_substring = available_date_substring[:available_date_substring.index(".")]
            available_date = available_date_substring.replace("Disponibile da ","").replace(".","").strip()
            available_date_month = available_date.split(" ")[0]
            available_date_year = available_date.split(" ")[1]
            available_date_number = self.convertItalianMonthToNum(available_date_month)
            available_date = str(available_date_year) + "-" + str(available_date_number) + "-01"
        all_details = apartment_info.xpath('.//div[contains(@id, "tab-details")]//div[contains(@class, "section")]')
        floor = all_details[0].xpath('.//li[2]//b/text()').extract()
        square_meters = int(all_details[1].xpath('.//li[2]//b/text()').extract()[0])
        furnished = None
        parking = None
        terrace = None
        balcony = None
        utilities = None
        elevator = None
        dishwasher = None
        room_count = 1
        bathroom_count = 0

        for div in all_details:
            each_div = div.xpath('.//h4/text()').extract()
            if each_div[0] in ['Caratteristiche Interne']:
                rows = div.xpath('.//li')
                for row in rows:
                    each_row = row.xpath('.//span/text()').extract()
                    if each_row[0] == "Bagni":
                        bathrooms_count = str(row.xpath('.//b/text()').extract())
                        all_room_count = [int(rooms) for rooms in bathrooms_count if rooms.isdigit()]
                        bathroom_count = sum(all_room_count)
                        if bathroom_count == 0:
                            bathroom_count = 1
                    elif each_row[0] == "Camere":
                        rooms_count = str(row.xpath('.//b/text()').extract())
                        rooms_count.replace(":", "")
                        all_room_count = [int(rooms) for rooms in rooms_count if rooms.isdigit()]
                        room_count = sum(all_room_count)
                        if room_count == 0:
                            room_count = 1
                    elif each_row[0] == "Arredato":
                        furnished = True
            elif each_div[0] == 'Caratteristiche Esterne':
                rows = div.xpath('.//li')
                for row in rows:
                    each_row = row.xpath('.//span/text()').extract()
                    each_row = each_row[0].replace("/", "")
                    if each_row in ["Parcheggio (Posti Auto)", "Garage"]:
                        parking = True
                    elif each_row in ["Terrazzoi"]:
                        terrace = True
                    elif each_row in ["Balconei"]:
                        balcony = True
                    elif each_row in ["Ascensore"]:
                        elevator = True
            elif each_div[0] == 'Caratteristiche impianti':
                rows = div.xpath('.//li')
                for row in rows:
                    each_row = row.xpath('.//span/text()').extract()
                    if each_row[0] == "Lavastoviglie":
                        dishwasher = True
            elif each_div[0] == 'Richiesta Economica':
                rows = div.xpath('.//li')
                for row in rows:
                    each_row = row.xpath('.//span/text()').extract()
                    if each_row[0] == "Spese mensili":
                        utilities_monthly = row.xpath('.//b/text()')[0].extract()
                        utilities_monthly = utilities_monthly.replace("€ ", "")
                        if utilities_monthly:
                            utilities = int(utilities_monthly)

        images_repeated = apartment_info.xpath('.//div[contains(@id, "property_images")]//.//div[contains(@class, "secondary")]//a/@href').extract()
        images = list(dict.fromkeys(images_repeated))
        property_type = 'apartment'
        rent = str(response.meta.get("rent")[0]).replace("€ ", "")
        rent = int(rent.replace(".", ""))
        correct_address = title.split(" a ")[1]
        lon, lat = extract_location_from_address(correct_address)
        latitude = str(lat)
        longitude = str(lon)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int
        if available_date:
            item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        # item_loader.add_value("deposit", rent)  # Int
        # item_loader.add_value("prepaid_rent", rent)  # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int
        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Civico19 Immobiliare') # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()

    def convertItalianMonthToNum(self,monthName):
        months = {
            "gennaio": "01",
            "febbraio": "02",
            "marzo": "03",
            "aprile": "04",
            "maggio": "05",
            "giugno": "06",
            "luglio": "07",
            "agosto": "08",
            "settembre": "09",
            "ottobre": "10",
            "novembre": "11",
            "dicembre": "12",
        }
        return months.get(str(monthName).lower())