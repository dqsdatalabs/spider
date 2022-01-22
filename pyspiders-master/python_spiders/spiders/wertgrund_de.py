# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas
import scrapy

from ..helper import extract_number_only, convert_to_numeric, extract_date, string_found, \
    extract_location_from_coordinates, extract_location_from_address
from ..loaders import ListingLoader


class WertgrundDeSpider(scrapy.Spider):
    name = "wertgrund_de"
    start_urls = [
        'https://www.wertgrund.de/immobilien-angebote/suchergebnisse-immobiliensuche/?type=Alle&listPostId=1354&exposePostId=1356&immoType=apartmentRent&ort=&immoSort=roomsDesc&search=Find+objects',

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
        apartment_page_links = response.xpath('//p[@class="ueberschrift"]//a')
        yield from response.follow_all(apartment_page_links, self.populate_item)

        for page_num in range(2, 6):
            next_page = f'https://www.wertgrund.de/immobilien-angebote/suchergebnisse-immobiliensuche/page/{page_num}/?type=Alle&listPostId=1354&exposePostId=1356&immoType=apartmentRent&ort&immoSort=roomsDesc&search=Find%2Bobjects'
            yield scrapy.Request(url=next_page, callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):

        title = response.xpath('//h1[@class="ueberschrift"]//text()').get()
        description = response.xpath('//div[@class="immobilienExposeSection"][2]//p//text()').getall()
        property_details = response.xpath('//div[@class="immobilienExposeSection"]//ul//text()').getall()

        details2 = response.xpath('//div[@class="immobilienExposeSection"]/p//text()').getall()
        amenities = " ".join(property_details) + " ".join(details2)

        floor = None
        if "Etage:" in property_details:
            position = property_details.index("Etage:")
            floor = (property_details[position + 1])

        rent = None
        if "Kaltmiete:" in property_details:
            position = property_details.index("Kaltmiete:")
            rent = int(convert_to_numeric(extract_number_only(property_details[position + 1])))

        deposit = None
        if "Kaution oder Genossenschaftsanteile:" in property_details:
            position = property_details.index("Kaution oder Genossenschaftsanteile:")
            deposit = property_details[position + 1]

        utilities = None
        if "Nebenkosten:" in property_details:
            position = property_details.index("Nebenkosten:")
            utilities = property_details[position + 1]

        square_meters = None
        if "Wohnfläche ca.:" in property_details:
            position = property_details.index("Wohnfläche ca.:")
            square_meters = int(convert_to_numeric(extract_number_only(property_details[position + 1])))

        room_count = None
        if "Zimmer:" in property_details:
            position = property_details.index("Zimmer:")
            room_count = convert_to_numeric(extract_number_only(property_details[position + 1]))
            x = isinstance(room_count, float)
            if x:
                room_count += 0.5

        bathroom_count = None
        if "Badezimmer:" in property_details:
            position = property_details.index("Badezimmer:")
            bathroom_count = convert_to_numeric(extract_number_only(property_details[position + 1]))
            if "Gäste-WC:" in amenities:
                bathroom_count = bathroom_count + 1

        available_date = None
        if "Bezugsfrei ab:" in property_details:
            position = property_details.index("Bezugsfrei ab:")
            available_date = extract_date(property_details[position + 1])

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

        property_type = 'apartment'
        location = response.xpath('//div[@class="entry-content"]/p/text()').get()

        longitude, latitude = extract_location_from_address(location)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        images = response.xpath('//div[@id="exposeSlider"]//ul[@class="slides"]//img//@src').getall()
        floor_plan_images = response.xpath('//ul[@class="immobilienExposeGrundriss"]//img//@src').getall()

        landlord_name = response.xpath('//span[@class="ansprechpartnerName"]//text()').get()
        # MetaData
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        # item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String

        # Property Details
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

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", '+49 6074 8407-0') # String
        item_loader.add_value("landlord_email", "info@wertgrund.de")  # String

        self.position += 1
        yield item_loader.load_item()
