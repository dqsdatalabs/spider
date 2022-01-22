# -*- coding: utf-8 -*-
# Author: A.Abbas
import scrapy

from ..helper import *
from ..loaders import ListingLoader


class RemaxCelleDeSpider(scrapy.Spider):
    name = "remax_celle_de"
    start_urls = [
        'https://remax-celle.de/immobilien/?post_type=immomakler_object&vermarktungsart=miete&nutzungsart=wohnen&typ=wohnung&ort&center&radius=25&objekt-id&collapse=out&von-qm=0.00&bis-qm=425.00&von-zimmer=0.00&bis-zimmer=16.00&von-kaltmiete=0.00&bis-kaltmiete=1100.00',
        'https://remax-celle.de/immobilien/?post_type=immomakler_object&vermarktungsart=miete&nutzungsart=wohnen&typ=haus&ort&center&radius=25&objekt-id&collapse&von-qm=0.00&bis-qm=425.00&von-zimmer=0.00&bis-zimmer=16.00&von-kaltmiete=0.00&bis-kaltmiete=1100.00',

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

        apartment_page_links = response.xpath('//div[@class="row immomakler-boxed"]//a')
        yield from response.follow_all(apartment_page_links, self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):

        title = response.xpath('//h1[@class="property-title"]//text()').get()
        location = response.xpath('//h2[@class="property-subtitle"]//text()').get()
        longitude, latitude = extract_location_from_address(location)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        floor_plan_images = response.xpath(
            '//div[@id="immomakler-galleria"]//a//img[@data-title="Grundriss"]//@src').get()
        all_images = response.xpath('//div[@id="immomakler-galleria"]//a//img//@data-title').getall()
        images = []
        for image in all_images:
            if image != "Grundriss" and image != "Energieskala":
                images.append(
                    response.xpath(f'//div[@id="immomakler-galleria"]//a//img[@data-title="{image}"]//@src').get())

        landlord_name = response.xpath(
            '//div[@class="property-contact panel panel-default"]//span[@class="p-name fn"]//text()').get()
        landlord_email = response.xpath(
            '//div[@class="property-contact panel panel-default"]//div[@class="row email"]//a//text()').getall()[-1]
        landlord_number = response.xpath(
            '//div[@class="property-contact panel panel-default"]//div[@class="row tel"]//a//text()').getall()[1]

        object_information = response.xpath(
            '//div[@class="col-xs-12 col-sm-5 col-sm-pull-7"]//ul[@class="list-group"]//text()').getall()
        details = "".join(response.xpath('//div[@class="col-xs-12"]//text()').getall())
        description = details.split("Ausstattung")[0]
        amenities = " ".join(object_information) + details

        external_id = None
        if "Objekt ID" in object_information:
            position = object_information.index("Objekt ID")
            external_id = object_information[position + 2]

        property_type = None
        if "Objekttypen" in object_information:
            position = object_information.index("Objekttypen")
            property_type = object_information[position + 2]

        if "Haus" in property_type:
            property_type = "house"
        else:
            property_type = "apartment"

        square_meters = None
        if "Wohnfläche\xa0ca." in object_information:
            position = object_information.index("Wohnfläche\xa0ca.")
            square_meters = object_information[position + 2].replace("m²", '').replace(" ", '')
            if "," in square_meters:
                square_meters = square_meters.split(',')[0]

        deposit = None
        if "Kaution" in object_information:
            position = object_information.index("Kaution")
            deposit = object_information[position + 2]

        rent = None
        if "Kaltmiete" in object_information:
            position = object_information.index("Kaltmiete")
            rent = object_information[position + 2]

        utilities = None
        if "Betriebskosten brutto" in object_information:
            position = object_information.index("Betriebskosten brutto")
            utilities = object_information[position + 2]

        energy_label = None
        if "Energie\xadeffizienz\xadklasse" in object_information:
            position = object_information.index("Energie\xadeffizienz\xadklasse")
            energy_label = object_information[position + 2]

        available_date = None
        if "Verfügbar ab" in object_information:
            position = object_information.index("Verfügbar ab")
            available_date = object_information[position + 2]

        room_count = 1
        if "Zimmer" in object_information:
            position = object_information.index("Zimmer")
            room_count = (object_information[position + 2])
            if "," in room_count:
                room_count = float(room_count.replace(",", '.')) + 0.5

        bathroom_count = None
        if "Badezimmer" in object_information:
            position = object_information.index("Badezimmer")
            bathroom_count = object_information[position + 2]

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
        #
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", remove_white_spaces(description))  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", location)  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type)  # String
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", extract_date(available_date))  # String => date_format
        #
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
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1
        yield item_loader.load_item()
