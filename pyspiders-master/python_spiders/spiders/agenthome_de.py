# -*- coding: utf-8 -*-
# Author: Mahmoud Wessam
import scrapy
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_coordinates


class AgenthomeDeSpider(scrapy.Spider):
    name = "agenthome_de"
    start_urls = ['https://www.agenthome.de/api/v2/list/stuttgart/de/full']
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
        parsed_response = json.loads(response.body)

        for item in parsed_response['apartments']:
            number = item['agnumber']
            images = item['images']
            link = f'https://www.agenthome.de/en/apartments/{item["location"]}/{number}'.replace(
                '\u00f6', 'oe')
            url = f'https://www.agenthome.de/api/v2/details/{number}/de'

            yield scrapy.Request(url=url,
                                 callback=self.populate_item,
                                 meta={"item": item, 'link': link, 'images': images},
                                 dont_filter=True
                                 )

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item = json.loads(response.body)

        zipcode, city, address = extract_location_from_coordinates(
            item['apartment']['gebaeude']['standort']['position']['lng'], item['apartment']['gebaeude']['standort']['position']['lat'])

        property_type = 'apartment'

        if "Studio" in item['apartment']['subline'] or "studio" in item['apartment']['subline']:
            property_type = 'studio'

        parking = None
        elevator = None
        balcony = None
        washing_machine = None
        dishwasher = None
        try:
            parking = item['apartment']['details']['Fahrradraum']
        except:
            parking = None

        try:
            elevator = item['apartment']['details']['Aufzug']
        except:
            elevator = None

        try:
            balcony = item['apartment']['details']['Balkon']
        except:
            balcony = None

        try:
            washing_machine = item['apartment']['details']['Waschmaschine']
        except:
            washing_machine = None

        try:
            dishwasher = item['apartment']['details']['Geschirrspüler']
        except:
            dishwasher = None

        rooms = item['apartment']['objektinformationen']['zimmerzahl']['value']
        if isinstance(rooms, str):
            rooms = int(rooms[0])+1

        images = response.meta['images']
        images = ['https://www.agenthome.de/application/files/apartments/' + image for image in images]

        # # MetaData
        item_loader.add_value("external_link", response.meta['link'])  # String
        item_loader.add_value(
            "external_source", self.external_source)  # String

        item_loader.add_value(
            "external_id", item['apartment']['agnr'])  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", item['apartment']['subline'])  # String
        item_loader.add_value(
            "description", item['apartment']['beschreibung'])  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value(
            "latitude", str(item['apartment']['gebaeude']['standort']['position']['lat']))  # String
        item_loader.add_value(
            "longitude", str(item['apartment']['gebaeude']['standort']['position']['lng']))  # String
        item_loader.add_value("floor", str(
            item['apartment']['etage']['value']))  # String
        item_loader.add_value("property_type", property_type)  # String
        item_loader.add_value(
            "square_meters", item['apartment']['objektinformationen']['wohnflaeche']['value'].split(' ')[0])  # Int
        item_loader.add_value("room_count", rooms)  # Int
        item_loader.add_value(
            "bathroom_count", item['apartment']['objektinformationen']['schlafzimmerzahl']['value'])  # Int

        # String => date_format
        item_loader.add_value(
            "available_date", item['apartment']['objektinformationen']['verfuegbar_ab']['value'])

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        # item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(
            item['apartment']['bilder']))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value(
            "rent", item['apartment']['objektinformationen']['monatsmiete']['value'].split(' ')[1])  # Int
        item_loader.add_value(
            "deposit", item['apartment']['weitere_leistungen']['kaution']['value'].split(' ')[1].replace('.', ''))  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Agent Home')  # String
        item_loader.add_value("landlord_phone", '+49 711 945 276 0')  # String
        item_loader.add_value("landlord_email", 'mail@agenthome.de')  # String

        self.position += 1
        yield item_loader.load_item()
