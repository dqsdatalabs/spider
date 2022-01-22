# -*- coding: utf-8 -*-
# Author: A.Abbas

import scrapy

from ..helper import *
from ..loaders import ListingLoader


class GibsonhomesDeSpider(scrapy.Spider):
    name = "gibsonhomes_de"
    start_urls = ["https://gibsonhomes.de/de/angebote.html"]
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
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

        for item in response.xpath('//div[@class="angebot"]//div[@class="acontainer"]'):
            url = item.xpath('.//div[@class="a1"]//a/@href').get()
            yield scrapy.Request("https://gibsonhomes.de/{}".format(url), callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.xpath('//div[@class="ueberschrift"]//text()').get()
        images = response.xpath('//div[@class="detail"]//img//@src').getall()
        for image in images:
            position = images.index(image)
            images[position] = "https://gibsonhomes.de/{}".format(image)

        all_information = response.xpath('//table//text()').getall()
        object_information = []

        for item in all_information:
            position = all_information.index(item)
            all_information[position] = remove_white_spaces(item).replace(":", '').lower()
            if not item.isspace():
                object_information.append(item.strip().lower().replace(":", ''))

        details = " ".join(all_information).lower().split("objektbeschreibung")[-1]
        amenities = details
        description = details.split("ausstattung")[0]

        street = None
        if "straße" in object_information:
            position = object_information.index("straße")
            street = (object_information[position + 1])

        location = None
        if "ort" in object_information:
            position = object_information.index("ort")
            location = (object_information[position + 1])
        full_location = location + street
        longitude, latitude = extract_location_from_address(full_location)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        external_id = None
        if "objekt id" in object_information:
            position = object_information.index("objekt id")
            external_id = (object_information[position + 1])

        floor = None
        if "etage" in object_information:
            position = object_information.index("etage")
            floor = (object_information[position + 1])

        property_type = None
        if "objektart" in object_information:
            position = object_information.index("objektart")
            property_type = (object_information[position + 1])
            check = string_found(['Büro', 'Lager', 'Garagen', 'Stellplatz'], property_type)
            if string_found(['wohnung', 'etagenwohnung'], property_type):
                property_type = 'apartment'
            elif string_found(['haus'], property_type):
                property_type = "house"
        elif "kategorie" in object_information:
            position = object_information.index("kategorie")
            property_type = (object_information[position + 1])
            check = string_found(['Büro', 'Lager', 'Garagen', 'Stellplatz'], property_type)
            if string_found(['wohnung', 'etagenwohnung'], property_type):
                property_type = 'apartment'
            elif string_found(['haus'], property_type):
                property_type = "house"

        square_meters = None
        if "wohnfläche ca." in object_information:
            position = object_information.index("wohnfläche ca.")
            square_meters = int(convert_to_numeric(extract_number_only(object_information[position + 1])))
        elif "wohnfläche" in object_information:
            position = object_information.index("wohnfläche")
            square_meters = int(convert_to_numeric(extract_number_only(object_information[position + 1])))

        room_count = None
        if "zimmer" in object_information:
            position = object_information.index("zimmer")
            room_count = convert_to_numeric(extract_number_only(object_information[position + 1].replace('.', ',')))
            check_room_type = isinstance(room_count, float)
            if check_room_type:
                room_count += 0.5
        elif "anzahl der zimmer gesamt" in object_information:
            position = object_information.index("anzahl der zimmer gesamt")
            room_count = convert_to_numeric(extract_number_only(object_information[position + 1].replace('.', ',')))
            check_room_type = isinstance(room_count, float)
            if check_room_type:
                room_count += 0.5

        bathroom_count = None
        if "badezimmer" in object_information:
            position = object_information.index("badezimmer")
            bathroom_count = convert_to_numeric(extract_number_only(object_information[position + 1].replace('.', ',')))
            check_bathroom_type = isinstance(bathroom_count, float)
            if check_bathroom_type:
                bathroom_count += 0.5
        elif "anzahl der bäder" in object_information:
            position = object_information.index("anzahl der bäder")
            bathroom_count = convert_to_numeric(extract_number_only(object_information[position + 1].replace('.', ',')))
            check_bathroom_type = isinstance(bathroom_count, float)
            if check_bathroom_type:
                bathroom_count += 0.5
        elif string_found(['badezimmer', 'bad'], amenities):
            bathroom_count = 1

        available_date = None
        if "bezugsfrei ab" in object_information:
            position = object_information.index("bezugsfrei ab")
            available_date = extract_date(object_information[position + 1])
        elif "verfügbar ab" in object_information:
            position = object_information.index("verfügbar ab")
            available_date = object_information[position + 1]

        rent = None
        if "kaltmiete" in object_information:
            position = object_information.index("kaltmiete")
            rent = object_information[position + 1].replace('.00', '')
        elif "miete" in object_information:
            position = object_information.index("miete")
            rent = extract_number_only(object_information[position + 1].replace('.00', '')).replace('.00', '')

        deposit = None
        if "kaution" in object_information:
            position = object_information.index("kaution")
            deposit = object_information[position + 1]
            if "monatsmieten kalt" in deposit:
                deposit = int(extract_number_only(rent)) * int(extract_number_only(deposit))

        utilities = None
        if "nebenkosten" in object_information:
            position = object_information.index("nebenkosten")
            utilities = object_information[position + 1].replace('.00', '')

        pets_allowed = False
        if string_found(['Haustiere: Nach Vereinbarung', 'haustiere erlaubt? ja'], amenities):
            pets_allowed = True

        furnished = False
        if string_found(['möblierte', 'möbliert'], amenities):
            furnished = True

        parking = False
        if string_found(
                ['Tiefgarage', 'Stellplatz', 'Garage', 'Tiefgaragenstellplatz', 'Außenstellplatz', 'Stellplätze',
                 'Einzelgarage', 'besucherparkplatz', 'parkplatz'], amenities):
            parking = True

        elevator = False
        if string_found(['Aufzug', 'Aufzügen', 'Fahrstuhl', 'Personenaufzug', 'Personenfahrstuhl'], amenities):
            elevator = True

        balcony = False
        if string_found(['Balkone', "Balkon", 'Südbalkon'], amenities):
            balcony = True

        terrace = False
        if string_found(['Terrassenwohnung', 'Terrasse', 'Terrasse (ca.)', 'Dachterrasse', 'Südterrasse'], amenities):
            terrace = True

        washing_machine = False
        if string_found(['gemeinschaftlicher Wasch', 'Trockenraum', 'Waschküche', 'Waschmaschinenzugang'], amenities):
            washing_machine = True

        dishwasher = False
        if string_found(['Spülmaschine', 'Geschirrspüler'], amenities):
            dishwasher = True
        if not check:
            # # MetaData
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value("external_source", self.external_source)  # String

            item_loader.add_value("external_id", remove_white_spaces(external_id))  # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", remove_white_spaces(title))  # String
            item_loader.add_value("description", remove_white_spaces(description))  # String

            # # Property Details
            item_loader.add_value("city", city)  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", address)  # String
            item_loader.add_value("latitude", str(latitude))  # String
            item_loader.add_value("longitude", str(longitude))  # String
            item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type", property_type)  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            item_loader.add_value("available_date", available_date)  # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
            item_loader.add_value("furnished", furnished)  # Boolean
            item_loader.add_value("parking", parking)  # Boolean
            item_loader.add_value("elevator", elevator)  # Boolean
            item_loader.add_value("balcony", balcony)  # Boolean
            item_loader.add_value("terrace", terrace)  # Boolean
            # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine)  # Boolean
            item_loader.add_value("dishwasher", dishwasher)  # Boolean

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            item_loader.add_value("deposit", deposit)  # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "EUR")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", remove_white_spaces('Gibson Homes'))  # String
            item_loader.add_value("landlord_phone", remove_white_spaces('0177 - 501 48 97'))  # String
            item_loader.add_value("landlord_email", 'info@gibsonhomes.de')  # String

            self.position += 1
            yield item_loader.load_item()
