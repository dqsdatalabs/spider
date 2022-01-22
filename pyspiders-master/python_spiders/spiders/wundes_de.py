# -*- coding: utf-8 -*-
# Author: A.Abbas
import scrapy

from ..helper import *
from ..loaders import ListingLoader


class WundesDeSpider(scrapy.Spider):
    name = "wundes_de"
    start_urls = ['https://www.wundes.de/ergebnisliste/?type=wohnungen&status=vermietung']
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

        for item in response.xpath('//div[@class="list-container clearfix"]//article'):
            url = item.xpath('.//a//@href').get()
            yield scrapy.Request(url, callback=self.populate_item)

        next_page = response.xpath('//div[@class="pagination"]//a//@href').getall()
        for page in next_page:
            yield scrapy.Request(page, callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        images = response.xpath('//ul[@class="slides"]//li//img//@src').getall()
        title = response.url.split("property/")[-1].replace("/", "").replace("-", ' ')

        location = response.xpath('//div[@class="outer-wrapper clearfix"]//address//text()').get()
        longitude, latitude = extract_location_from_address(location)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        object_information = response.xpath('//div[@class="content clearfix"]//ul//text()').getall()
        description = " ".join(response.xpath('//div[@class="content clearfix"]//text()').getall()).split("Lage")[0]
        details = " ".join(response.xpath('//div[@class="content clearfix"]//text()').getall())
        amenities = details + " ".join(object_information)

        external_id = remove_white_spaces("".join(
            response.xpath('//div[@class="property-meta clearfix"]//span[@title="Immobilien-ID"]//text()').getall()))

        square_meters = \
            response.xpath('//div[@class="property-meta clearfix"]//span[@title="Wohnfläche"]//text()').getall()[-1]

        check_for_the_bathroom = "".join(response.xpath('//div[@class="property-meta clearfix"]//text()').getall())
        if "Badezimmer" in check_for_the_bathroom:
            bathroom = response.xpath('//div[@class="property-meta clearfix"]//span[4]//text()').getall()[-1]
            bathroom_count = extract_number_only(bathroom)
            if bathroom is not None and "," in bathroom:
                bathroom_count = int(extract_number_only(bathroom)) + 0.5

        elif string_found(['Bad', 'Badausstattung', 'Badezimmer'], amenities):
            bathroom_count = 1
        else:
            bathroom_count = None

        deposit = None
        if "Kaution:" in object_information:
            position = object_information.index("Kaution:")
            deposit = object_information[position + 2]

        rent = None
        if "Kaltmiete:" in object_information:
            position = object_information.index("Kaltmiete:")
            rent = object_information[position + 2]

        available_date = None
        if "Immobilie ist verfügbar ab:" in object_information:
            position = object_information.index("Immobilie ist verfügbar ab:")
            available_date = object_information[position + 2]

        floor = None
        if "Etage" in object_information:
            position = object_information.index("Etage")
            floor = (object_information[position + 2])

        utilities = None
        if "monatl. Betriebs-/Nebenkosten:" in object_information:
            position = object_information.index("monatl. Betriebs-/Nebenkosten:")
            utilities = object_information[position + 2]

        room_count = 1
        if "Zimmer insgesamt:" in object_information:
            position = object_information.index("Zimmer insgesamt:")
            room_count = (object_information[position + 2])
            if "," in room_count:
                room_count = float(extract_number_only(room_count)) + 0.5

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
        energy_label = response.xpath('//div[@class="current-energy-class"]//text()').get()

        # # MetaData
        if rent is not None:
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
            item_loader.add_value("property_type", "apartment")  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            item_loader.add_value("available_date", extract_date(available_date))  # String => date_format

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
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value("landlord_name", 'WUNDES & PARTNER')  # String
            item_loader.add_value("landlord_phone", "02129/9499-0")  # String
            item_loader.add_value("landlord_email", 'info@wundes.de')  # String

            self.position += 1
            yield item_loader.load_item()
