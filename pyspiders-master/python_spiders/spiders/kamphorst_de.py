# -*- coding: utf-8 -*-
# Author: A.Abbas

import scrapy

from ..helper import *
from ..loaders import ListingLoader


class KamphorstDeSpider(scrapy.Spider):
    name = "kamphorst_de"
    start_urls = [
        'https://kamphorst.de/immobilien/suche.html',
        'https://kamphorst.de/immobilien/suche.html?start=6',


    ]
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

        for item in response.xpath('//div[@id="prim_mainheader"]//div[@class="col-md-12"]'):
            urls = item.xpath('.//div[@class="img-polaroid"]//a//@href').get()
            yield scrapy.Request(url="https://kamphorst.de{}".format(urls), callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):

        title = remove_white_spaces(response.xpath('//h2//text()').getall()[-1])
        images = response.xpath('//img[@class="prima-gal"]//@src').getall()
        for image in images:
            position = images.index(image)
            images[position] = "https://kamphorst.de/{}".format(image)

        latitude = \
            response.xpath('//*[@id="t3-content"]/script[1]//text()').get().split(")")[0].split('(')[-1].split(",")[0]
        longitude = \
            response.xpath('//*[@id="t3-content"]/script[1]//text()').get().split(")")[0].split('(')[-1].split(",")[
                1].replace(" ", '')
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        landlord_name = response.xpath('//h3[@class="title notranslate"]//text()').get()
        landlord_number = remove_white_spaces("".join(response.xpath(
            '//div[@id="prima-contact"]//div[@class="col-lg-5 col-md-6"]//div[@class="col-12"]//text()').getall())).split(
            'E-Mail')[0].split("Telefon:")[-1]
        external_id = response.xpath('//div[@class="ezitem-legend"]//span//text()').get().replace("ObjektNummer:",'')

        details = remove_white_spaces(" ".join(response.xpath('//div[@class="col-12"]//text()').getall())).split(
            "document.getElementById")[0]
        description = \
            " ".join(response.xpath('//div[@class="col-12"]//text()').getall()).split('OBJEKTBESCHREIBUNG:')[-1].split(
                "AUSSTATTUNG:")[0]
        all_information = response.xpath('//table[@class="table"]//text()').getall()
        object_information = []
        amenities = details.lower()

        for item in all_information:
            position = all_information.index(item)
            all_information[position] = remove_white_spaces(item)
            if not item.isspace():
                object_information.append(item.strip().lower().replace(":", ''))

        property_type = None
        if "objektart" in object_information:
            position = object_information.index("objektart")
            property_type = (object_information[position + 1])
            if string_found(['wohnung', 'etagenwohnung'], property_type):
                property_type = 'apartment'
            elif string_found(['haus'], property_type):
                property_type = "house"

        floor = None
        if "etage" in object_information:
            position = object_information.index("etage")
            floor = (object_information[position + 1])

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

        bathroom_count = None
        if "badezimmer" in object_information:
            position = object_information.index("badezimmer")
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

        rent = None
        if "kaltmiete" in object_information:
            position = object_information.index("kaltmiete")
            rent = object_information[position + 1].replace('.00', '')

        deposit = None
        if "kaution" in object_information:
            position = object_information.index("kaution")
            deposit = object_information[position + 1]

        utilities = None
        if "nebenkosten" in object_information:
            position = object_information.index("nebenkosten")
            utilities = object_information[position + 1].replace('.00', '')

        energy_label = None
        if "energie\xadeffizienz\xadklasse" in object_information:
            position = object_information.index("energie\xadeffizienz\xadklasse")
            energy_label = object_information[position + 1].upper()

        elevator = False
        if string_found(['Aufzug', 'Aufzügen', 'Fahrstuhl', 'Personenaufzug', 'Personenfahrstuhl'], amenities):
            elevator = True

        parking = False
        if string_found(
                ['Tiefgarage', 'Stellplatz', 'Garage', 'Tiefgaragenstellplatz', 'Außenstellplatz', 'Stellplätze',
                 'Einzelgarage'], amenities):
            parking = True

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

        pets_allowed = False
        if string_found(['Haustiere: Nach Vereinbarung'], amenities):
            pets_allowed = True

        # # MetaData
        if "apartment" == property_type and "kaufpreis" not in object_information:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value("external_source", self.external_source)  # String

            item_loader.add_value("external_id", remove_white_spaces(external_id))  # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title)  # String
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
            # item_loader.add_value("furnished", furnished) # Boolean
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

            item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value("landlord_name", remove_white_spaces(landlord_name))  # String
            item_loader.add_value("landlord_phone", remove_white_spaces(landlord_number))  # String
            item_loader.add_value("landlord_email", "el@kamphorst.de") # String

            self.position += 1
            yield item_loader.load_item()
