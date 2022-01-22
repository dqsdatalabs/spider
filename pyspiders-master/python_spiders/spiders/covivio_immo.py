# -*- coding: utf-8 -*-
# Author: A.Abbas
import json

import scrapy

from ..helper import *
from ..loaders import ListingLoader


class CovivioImmoSpider(scrapy.Spider):
    name = "covivio_immo"
    start_urls = [
        'https://www.covivio.immo/wp-json/wp/v2/objekt?anzahl_zimmer_max=6&anzahl_zimmer_min=0&distance=10&kaltmiete_max=16500&kaltmiete_min=25&&objektart=wohnung&order=asc&orderby=distance&per_page=10&wohnflaeche_max=241&wohnflaeche_min=0&page=1',
        'https://www.covivio.immo/wp-json/wp/v2/objekt?anzahl_zimmer_max=6&anzahl_zimmer_min=0&distance=10&googlemapsEnabled=true&kaltmiete_max=16500&kaltmiete_min=25&&objektart=wohnung&order=asc&orderby=distance&per_page=10&wohnflaeche_max=241&wohnflaeche_min=0&page=2',
        'https://www.covivio.immo/wp-json/wp/v2/objekt?anzahl_zimmer_max=6&anzahl_zimmer_min=0&distance=10&googlemapsEnabled=true&kaltmiete_max=16500&kaltmiete_min=25&&objektart=wohnung&order=asc&orderby=distance&per_page=10&wohnflaeche_max=241&wohnflaeche_min=0&page=3',
        'https://www.covivio.immo/wp-json/wp/v2/objekt?anzahl_zimmer_max=6&anzahl_zimmer_min=0&distance=10&googlemapsEnabled=true&kaltmiete_max=16500&kaltmiete_min=25&&objektart=wohnung&order=asc&orderby=distance&per_page=10&wohnflaeche_max=241&wohnflaeche_min=0&page=4',
        'https://www.covivio.immo/wp-json/wp/v2/objekt?anzahl_zimmer_max=6&anzahl_zimmer_min=0&distance=10&googlemapsEnabled=true&kaltmiete_max=16500&kaltmiete_min=25&&objektart=wohnung&order=asc&orderby=distance&per_page=10&wohnflaeche_max=241&wohnflaeche_min=0&page=5',
        'https://www.covivio.immo/wp-json/wp/v2/objekt?anzahl_zimmer_max=6&anzahl_zimmer_min=0&distance=10&googlemapsEnabled=true&kaltmiete_max=16500&kaltmiete_min=25&&objektart=wohnung&order=asc&orderby=distance&per_page=10&wohnflaeche_max=241&wohnflaeche_min=0&page=6',
        'https://www.covivio.immo/wp-json/wp/v2/objekt?anzahl_zimmer_max=6&anzahl_zimmer_min=0&distance=10&googlemapsEnabled=true&kaltmiete_max=16500&kaltmiete_min=25&&objektart=wohnung&order=asc&orderby=distance&per_page=10&wohnflaeche_max=241&wohnflaeche_min=0&page=7',
        'https://www.covivio.immo/wp-json/wp/v2/objekt?anzahl_zimmer_max=6&anzahl_zimmer_min=0&distance=10&googlemapsEnabled=true&kaltmiete_max=16500&kaltmiete_min=25&&objektart=wohnung&order=asc&orderby=distance&per_page=10&wohnflaeche_max=241&wohnflaeche_min=0&page=8',
        'https://www.covivio.immo/wp-json/wp/v2/objekt?anzahl_zimmer_max=6&anzahl_zimmer_min=0&distance=10&googlemapsEnabled=true&kaltmiete_max=16500&kaltmiete_min=25&&objektart=wohnung&order=asc&orderby=distance&per_page=10&wohnflaeche_max=241&wohnflaeche_min=0&page=9',
        'https://www.covivio.immo/wp-json/wp/v2/objekt?anzahl_zimmer_max=6&anzahl_zimmer_min=0&distance=10&googlemapsEnabled=true&kaltmiete_max=16500&kaltmiete_min=25&&objektart=wohnung&order=asc&orderby=distance&per_page=10&wohnflaeche_max=241&wohnflaeche_min=0&page=10',
        'https://www.covivio.immo/wp-json/wp/v2/objekt?anzahl_zimmer_max=6&anzahl_zimmer_min=0&distance=10&googlemapsEnabled=true&kaltmiete_max=16500&kaltmiete_min=25&&objektart=wohnung&order=asc&orderby=distance&per_page=10&wohnflaeche_max=241&wohnflaeche_min=0&page=11',

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
        data = json.loads(response.body)
        for item in data:
            link = item['link']
            yield scrapy.Request(url=link, callback=self.populate_item, meta={"item": item})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        data = response.meta['item']

        title = data['title']["rendered"].replace("*", "")

        latitude = data['location']['lat']
        longitude = data['location']['lng']
        zipcode, cty, location = extract_location_from_coordinates(longitude, latitude)
        address = data["adresse"]
        city = data["ort"]

        rent = data["kaltmiete"]

        square_meters = data["wohnflaeche"]

        room_count = data["anzahl_zimmer"]

        property_type = data["objektart"]

        if "zimmer" in property_type.lower():
            property_type = "room"
        elif "haus" in property_type.lower():
            property_type = "house"
        else:
            property_type = "apartment"

        images_data = data["bilder"]
        images = []
        for image in images_data:
            if image["titel"] != "Auszeichnung":
                images.append(image["url"])
        floor_plan_images = response.xpath('//img[@class="img-fluid grundriss"]//@data-lazy-src').get()

        object_information = remove_white_spaces(
            " ".join(response.xpath('//div[@class="details"]/div[@class="row"]//text()').getall())).split(" ")
        details = " ".join(response.xpath('//div[@class="freitexte"]//text()').getall())
        amenities = details

        description = details.split("Sonstiges")[0]

        bathroom_count = False
        if string_found(['WC', 'Bad mit'], amenities):
            bathroom_count = 1

        external_id = None
        if "Objektnummer" in object_information:
            position = object_information.index("Objektnummer")
            external_id = object_information[position + 1]

        utilities = None
        if "Betriebskosten" in object_information:
            position = object_information.index("Betriebskosten")
            utilities = object_information[position + 1]

        deposit = None
        if "Kaution" in object_information:
            position = object_information.index("Kaution")
            deposit = object_information[position + 1]

        heating_cost = None
        if "Heizkosten" in object_information:
            position = object_information.index("Heizkosten")
            heating_cost = object_information[position + 1]

        floor = None
        if "Etage" in object_information:
            position = object_information.index("Etage")
            floor = (object_information[position + 1])

        available_date = None
        if "Frei" in object_information:
            position = object_information.index("Frei")
            available_date = extract_date(object_information[position + 2])

        landlord_email = None
        if "E-Mail" in object_information:
            position = object_information.index("E-Mail")
            landlord_email = (object_information[position + 1])

        landlord_number = None
        if "Telefon" in object_information:
            position = object_information.index("Telefon")
            landlord_number = (object_information[position + 1])

        landlord_name = None
        if "Ansprechpartner" in object_information:
            position1 = object_information.index("Ansprechpartner")
            position2 = object_information.index("Anschrift")
            position = position2 - position1
            landlord_name = " ".join((object_information[position1 + 1:position1 + position]))

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
        item_loader.add_value("heating_cost", heating_cost)  # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1
        yield item_loader.load_item()
