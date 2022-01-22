# -*- coding: utf-8 -*-
# Author: A.Abbas
import scrapy

from ..helper import *
from ..loaders import ListingLoader


class MannellaImmobilienDeSpider(scrapy.Spider):
    name = "mannella_immobilien_de"
    start_urls = [
        'https://www.mannella-immobilien.de/immobilien/mieten/wohnungen',
        'https://www.mannella-immobilien.de/immobilien/mieten/wohnungen,o8',
        'https://www.mannella-immobilien.de/immobilien/mieten/haeuser'
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
        for item in response.xpath('//div[@class="clearfix listing"]//div[@class="listing-object-wrapper"]'):
            url = item.xpath('.//a[@class="no-hover"]//@href').get()
            rent = item.xpath('.//li[@class="price"]//span//text()').get()
            check_for_rented = "".join(item.xpath('.//div[@class="object-context-wrapper"]//p[2]//text()').getall())
            if "vermietet" not in check_for_rented:
                yield scrapy.Request("https://www.mannella-immobilien.de{}".format(url), callback=self.populate_item,
                                     meta={
                                         "rent": rent,
                                     })

    # 3. SCRAPING level 3
    def populate_item(self, response):

        latitude = \
            response.xpath('//*[@id="main"]/div/div/div[2]/div/script[1]//text()').get().split('map",')[-1].split(',')[
                0].replace(' ', '')
        longitude = \
            response.xpath('//*[@id="main"]/div/div/div[2]/div/script[1]//text()').get().split('map",')[-1].split(',')[
                1].replace(' ', '')
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        title = response.xpath('//h1//text()').get()

        external_id = remove_white_spaces(
            response.xpath('//div[@class="col-md-5 col-sm-5 col-xs-12"]//p//text()').get().replace('Objekt-Nr.', ''))

        floor_plan_images = response.xpath('//a[@title="Grundriss"]//@href').get()
        all_images = response.xpath(
            '//div[@class="carousel slide"]//a[@class="sc-media-gallery embed-responsive-item"]//@title').getall()
        images = []
        for image in all_images:
            if image != "Grundriss" and image != "Interesse geweckt" and image != "MANNELLA Immobilienservice Hennef":
                images.append(response.xpath(f'//div[@class="carousel slide"]//a[@title="{image}"]//@href').get())

        landlord_name = response.xpath(
            '//div[@class="panel panel-default"]//div[@class="panel-body"]//p//strong//text()').get()
        landlord_email = response.xpath(
            '//div[@class="panel panel-default"]//div[@class="panel-body"]//a[@class="inline-popup"]//text()').get()
        landlord_number = "".join(
            response.xpath('//div[@class="panel panel-default"]//div[@class="panel-body"]//p/text()').getall()).replace(
            "Telefon:", "")

        object_information = response.xpath('//table[@class="table table-condensed equipment"]//text()').getall()
        object_information2 = response.xpath('//table[@class="table table-condensed"]//text()').getall()

        details2 = response.xpath('//div[@class="col-md-12 col-sm-12 col-xs-12"]//text()').getall()
        description = remove_white_spaces(" ".join(details2))
        if "Sonstiges" in description:
            description = description.split("Sonstiges")[0]
        else:
            description = description.split("Lagebeschreibung")[0]

        amenities = " ".join(object_information) + " ".join(details2)

        for item in object_information:
            position = object_information.index(item)
            object_information[position] = remove_white_spaces(item)

        for item in object_information2:
            position = object_information2.index(item)
            object_information2[position] = remove_white_spaces(item)

        deposit = None
        if "Kaution" in object_information:
            position = object_information.index("Kaution")
            deposit = object_information[position + 1]

        utilities = None
        if "Nebenkosten" in object_information:
            position = object_information.index("Nebenkosten")
            utilities = object_information[position + 1]

        bathroom_count = None
        if "Anzahl Badezimmer" in object_information:
            position = object_information.index("Anzahl Badezimmer")
            bathroom_count = object_information[position + 1]
        # elif "Bad" in object_information:
        #     bathroom_count = 1

        available_date = None
        if "verfügbar ab" in object_information:
            position = object_information.index("verfügbar ab")
            available_date = object_information[position + 1]

        square_meters = None
        if "Wohnfläche" in object_information:
            position = object_information.index("Wohnfläche")
            square_meters = int(convert_to_numeric(extract_number_only(object_information[position + 1])))

        room_count = None
        if "Zimmer" in object_information:
            position = object_information.index("Zimmer")
            room_count = (object_information[position + 1])
            if "," in room_count:
                room_count = float(room_count.replace(",", '.')) + 0.5

        energy_label = None
        if "Energieeffizienzklasse" in object_information2:
            position = object_information2.index("Energieeffizienzklasse")
            energy_label = object_information2[position + 1]

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

        property_type = "apartment"
        if "Mietshaus" in title:
            property_type = "house"

        # # MetaData
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", remove_white_spaces(description))  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type)  # String
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
        item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # # Monetary Status
        item_loader.add_value("rent", response.meta["rent"])  # Int
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
