# -*- coding: utf-8 -*-
# Author: A.Abbas
import scrapy
import w3lib.html

from ..helper import *
from ..loaders import ListingLoader


class KringsImmobilienDeSpider(scrapy.Spider):
    name = "krings_immobilien_de"
    start_urls = [
        'https://www.krings-immobilien.de/immobilienangebote/?_search=true&mt=rent&category=14&address=&sort=sort%7Cdesc#immobilien']
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
        apartment_page_links = response.xpath('//a[@class="immo-listing__image"]')
        yield from response.follow_all(apartment_page_links, self.populate_item)

        next_page = response.xpath('//a[@class="next page-numbers"]//@href').get()
        if next_page is not None:
            yield scrapy.Request(next_page, callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):

        details = w3lib.html.remove_tags(response.xpath('//script[@class="vue-tabs"]//text()').get()).split(" ")
        object_information = remove_white_spaces(
            w3lib.html.remove_tags(response.xpath('//script[@class="vue-tabs"]//text()').getall()[1])).split(" ")
        amenities = " ".join(details) + " ".join(object_information)

        location = response.xpath('//div[@class="container pt-6"]/p/text()').get() + " DEUTSCHLAND"
        longitude, latitude = extract_location_from_address(location)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        if convert_to_numeric(address):
            address = location
        if address == "Lush":
            address = location

        title = response.xpath(
            '//div[@class="container pt-6"]//div[@class="col-24 col-lg-16 py-1 py-lg-0"]//h1//text()').get()
        description = remove_white_spaces(" ".join(details).split('Ausstattung')[0])

        images = response.xpath('//div[@id="exGallery"]//a[not(@title="Grundriss Wohnung")]//@href').getall()
        floor_plan_images = response.xpath('//div[@id="exGallery"]//a[@title="Grundriss Wohnung"]//@href').get()

        rent = response.xpath('//div[@class="immo-listing__infotext my-3"]/div//div[1]//li[1]//span//text()').get()
        external_id = remove_white_spaces(response.xpath(
            '//div[@class="immo-listing__infotext my-3"]/div//div[1]//li[2]//span//text()').get().replace("Objekt-Nr.:",
                                                                                                          ''))
        room_count = extract_number_only(
            response.xpath('//div[@class="immo-listing__infotext my-3"]/div//div[2]//li[1]//span//text()').get())
        if "." in room_count:
            room_count = float(room_count)
            room_count += 0.5

        pro_type = response.xpath('//div[@class="container pt-6"]/p//span/text()').get()

        if "zimmer" in pro_type.lower():
            property_type = "room"
        elif "haus" in pro_type.lower():
            property_type = "house"
        else:
            property_type = "apartment"

        deposit = None
        if "Kaution:" in object_information:
            position = object_information.index("Kaution:")
            deposit = object_information[position + 1]

        utilities = None
        if "Nebenkosten:" in object_information:
            position = object_information.index("Nebenkosten:")
            utilities = object_information[position + 1]

        floor = None
        if "Etagen:" in object_information:
            position = object_information.index("Etagen:")
            floor = (object_information[position + 1])

        bathroom_count = None
        if "Badezimmer:" in object_information:
            position = object_information.index("Badezimmer:")
            bathroom_count = (object_information[position + 1])
        elif "Bad" in object_information:
            bathroom_count = 1

        available_date = None
        if "verfügbar:" in object_information:
            position = object_information.index("verfügbar:")
            available_date = object_information[position + 2]

        square_meters = None
        if "Wohnfläche&nbsp;(ca.):" in object_information:
            position = object_information.index("Wohnfläche&nbsp;(ca.):")
            square_meters = int(convert_to_numeric(extract_number_only(object_information[position + 1])))

        energy_label = None
        if "Energieeffizienzklasse" in object_information:
            position = object_information.index("Energieeffizienzklasse")
            energy_label = (object_information[position + 1])

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
        if rent != "Preis auf Anfrage":
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value("external_source", self.external_source)  # String

            item_loader.add_value("external_id", external_id)  # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String

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
            item_loader.add_value("landlord_name", "KRINGS IMMOBILIEN GMBH")  # String
            item_loader.add_value("landlord_phone", " 02171 / 39 89 - 0")  # String
            item_loader.add_value("landlord_email", "info@krings-immobilien.de")  # String

            self.position += 1
            yield item_loader.load_item()
