# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_location_from_address


class AlfredpfeifferimmobilienPyspiderGermanySpider(scrapy.Spider):
    name = "AlfredPfeifferImmobilien"
    start_urls = [
        'https://portal.immobilienscout24.de/ergebnisliste/66745188',
        'https://immobilien.stuttgarter-zeitung.de/anbieter/alfred-pfeiffer-immobilien-gmbh-2614'
    ]
    allowed_domains = ["pfeiffer-immobilien.de", "immobilien.stuttgarter-zeitung.de", "portal.immobilienscout24.de"]
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
        if response.url == "https://portal.immobilienscout24.de/ergebnisliste/66745188":
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
                    apartment_title = apartment_div.xpath('.//div[contains(@class, "result__list__element--infos")]//h3//a/text()')[0].extract()
                    apartment_balcony = apartment_div.xpath('.//div[contains(@class, "result__list__element--infos")]//h3//em[contains(@class, "is24portale-tag19")]/text()').extract()
                    address = apartment_div.xpath('.//div[contains(@class, "result__list__element--infos")]//div[contains(@class, "result__list__element__infos--location")]//p/text()')[0].extract()
                    rent = apartment_div.xpath('.//div[contains(@class, "result__list__element--infos")]//ul//li[1]//span/text()')[0].extract()
                    square_meters = apartment_div.xpath('.//div[contains(@class, "result__list__element--infos")]//ul//li[2]//span/text()')[0].extract()
                    room_count = apartment_div.xpath('.//div[contains(@class, "result__list__element--infos")]//ul//li[3]//span/text()')[0].extract()
                    yield scrapy.Request(url, callback=self.populate_item_one, meta={
                        "title": apartment_title,
                        "balcony": apartment_balcony,
                        "address": address,
                        "rent": rent,
                        "square_meters": square_meters,
                        "room_count": room_count,
                    })
        elif response.url == "https://immobilien.stuttgarter-zeitung.de/anbieter/alfred-pfeiffer-immobilien-gmbh-2614":
            apartments_divs = response.xpath('.//div[contains(@id, "profile-item-list--living")]//div[contains(@class, "item-wrap")]')
            for apartment_div in apartments_divs:
                apartment_title = apartment_div.xpath('.//td[contains(@class, "item__hidden-header__title")]//a/text()')[0].extract()
                apartments_url = apartment_div.xpath('.//td[contains(@class, "item__hidden-header__title")]//a/@href').extract()
                url = "https://immobilien.stuttgarter-zeitung.de" + apartments_url[0]
                rent = apartment_div.xpath('normalize-space(.//div[contains(@class, "item-spec-price")]/text())')[0].extract()
                room_count = apartment_div.xpath('normalize-space(.//div[contains(@class, "item-spec-rooms")]/text())')[0].extract()
                square_meters = apartment_div.xpath('normalize-space(.//div[contains(@class, "item-spec-area")]/text())')[0].extract()
                lat_lng = apartment_div.xpath('./@data-lat-lng').extract()
                address = apartment_div.xpath('.//div[contains(@class, "item__locality")]/text()')[1].extract()
                address = address.strip()
                yield scrapy.Request(url, callback=self.populate_item_two, meta={
                    "title": apartment_title,
                    "address": address,
                    "rent": rent,
                    "square_meters": square_meters,
                    "room_count": room_count,
                    "lat_lng": lat_lng,
                })

    # 3. SCRAPING level 3
    def populate_item_one(self, response):
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
                        if "ab sofort" in available_date:
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

        landlord_name = "Alfred Pfeiffer Immobilien GmbH"
        landlord_number = "0711 621961"
        landlord_email = "mail@pfeiffer-immobilien.de"

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
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
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
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

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()

    # 4. SCRAPING level 4
    def populate_item_two(self, response):
        item_loader = ListingLoader(response=response)

        title = response.meta.get("title")

        rent = response.meta.get("rent")
        rent = rent.replace("€", "")
        rent = rent.replace(".", "")
        rent = rent.replace(",", ".")
        rent = float(rent)
        rent = round(rent)
        rent = int(rent)

        square_meters = response.meta.get("square_meters")
        square_meters = square_meters.replace("m", "")
        square_meters = int(square_meters)

        room_count = response.meta.get("room_count")
        room_count = room_count.replace("Zi.", "")
        room_count = room_count.replace(",", ".")
        room_count = float(room_count)
        room_count = round(room_count)
        room_count = int(room_count)

        property_type = "apartment"

        apartment_data = response.xpath('.//div[contains(@id, "edc-data")]//div[contains(@class, "row")]')
        floor = None
        available_date = None
        external_id = None
        for item in apartment_data:
            data_label = item.xpath('.//div[contains(@class, "col-label")]/text()').extract()
            data_value = item.xpath('.//div[contains(@class, "col-value")]/text()').extract()
            if "Etage" in data_label:
                floor = data_value
            elif "verfügbar ab" in data_label:
                available_date = data_value[0]
                if "ab sofort" in available_date:
                    available_date = None
                else:
                    available_date = available_date.split(".")
                    day = available_date[0]
                    month = available_date[1]
                    year = available_date[2]
                    available_date = year + "-" + month + "-" + day
            elif "Online-ID" in data_label:
                external_id = data_value

        apartment_prices = response.xpath('.//div[contains(@id, "ecc-data")]//div[contains(@class, "row")]')
        deposit = None
        utilities = None
        heating_cost = None
        for item in apartment_prices:
            data_label = item.xpath('normalize-space(.//div[contains(@class, "col-label")]/text())')[0].extract()
            data_value = item.xpath('normalize-space(.//div[contains(@class, "col-value")]/text())')[0].extract()
            if "Kaution" in data_label:
                deposit = data_value
                deposit = (deposit.split())[0]
                deposit = int(deposit) * rent
            elif "Warm" in data_label:
                heating_cost = data_value
                heating_cost = heating_cost.replace("€", "")
                heating_cost = heating_cost.replace(".", "")
                heating_cost = heating_cost.replace(",", ".")
                heating_cost = float(heating_cost)
                heating_cost = round(heating_cost)
                heating_cost = int(heating_cost) - rent
            elif "Betriebs" in data_label:
                utilities = data_value
                utilities = utilities.replace("€", "")
                utilities = utilities.replace(".", "")
                utilities = utilities.replace(",", ".")
                utilities = float(utilities)
                utilities = round(utilities)
                utilities = int(utilities)

        apartment_furnishing = response.xpath('.//div[contains(@id, "edc-equipment")]//div[contains(@class, "row")]')
        bathroom_count = None
        balcony = None
        terrace = None
        parking = None
        for item in apartment_furnishing:
            data_label = item.xpath('.//div[contains(@class, "col-label")]/text()').extract()
            data_value = item.xpath('.//div[contains(@class, "col-value")]/text()').extract()
            if "Badezimmer" in data_label:
                bathroom_count = data_value[0]
                bathroom_count = int(bathroom_count)
            elif "Balkon / Terrasse" in data_label:
                balcony = True
                terrace = True
            elif "Parkett" in data_label:
                parking = True

        apartment_energy = response.xpath('.//div[contains(@id, "edc-energy-pass")]//div[contains(@class, "row")]')
        energy_label = None
        for item in apartment_energy:
            data_label = item.xpath('.//div[contains(@class, "col-label")]/text()').extract()
            data_value = item.xpath('.//div[contains(@class, "col-value")]/text()').extract()
            if "Energieeffizienzklasse" in data_label:
                energy_label = data_value[0]

        description = response.xpath('normalize-space(.//div[contains(@class, "edc-description-inner")]//p[1]/text())').extract()

        coordinates = response.xpath('.//div[contains(@class, "map-single")]/@data-center')[0].extract()
        coordinates = coordinates.split(",")
        latitude = coordinates[0].split(":")
        latitude = latitude[1]
        latitude = str(latitude)
        longitude = coordinates[1].split(":")
        longitude = longitude[1]
        longitude = str(longitude)
        longitude = longitude.replace("}", "")
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        landlord_name = "Alfred Pfeiffer Immobilien GmbH"
        landlord_number = "0711 621961"
        landlord_email = "mail@pfeiffer-immobilien.de"

        images_list = response.xpath('.//div[contains(@class, "photo-gallery")]/@data-items').extract()
        images_all = []
        images_no = images_list[0].replace("[", "")
        images_no = images_no.replace("]", "")
        images_no = images_no.replace("{", "")
        images_no = images_no.replace("}", "")
        images_no = images_no.split("src")
        for image in images_no:
            if "preview" in image:
                image = image.split("preview")
                image = image[1]
                image = image.split("thumb")
                image = image[0]
                images_all.append(image)
        images = []
        for image in images_all:
            image = image.replace("\\", "")
            image = image[image.index("https"):]
            image = image.replace('","', '')
            images.append(image)

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
        #item_loader.add_value("elevator", elevator) # Boolean
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
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
