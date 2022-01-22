# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy

from ..helper import *
from ..loaders import ListingLoader


class ImmoTrend24ComSpider(scrapy.Spider):
    name = "immo_trend24_com"
    start_urls = [
        "https://immo-trend24.de/immobilien/?post_type=immomakler_object&vermarktungsart=miete&nutzungsart=wohnen&typ&ort&center&radius=25&objekt-id&collapse&von-qm=0.00&bis-qm=2545.00&von-zimmer=0.00&bis-zimmer=37.00&von-kaltmiete=0.00&bis-kaltmiete=4500.00&von-kaufpreis=0.00&bis-kaufpreis=3800000.00"]
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
        for item in response.xpath('//div[@class="property"]/div'):
            url = item.xpath('.//a/@href').get()
            title = item.xpath('.//h3//text()').get()
            external_id = item.xpath('.//div[@class="property-data"]/div[1]/div[2]//text()').get()
            room_count = item.xpath('.//div[@class="property-data"]/div[2]/div[2]//text()').get()
            square_meters = item.xpath('.//div[@class="property-data"]/div[3]/div[2]//text()').get()
            available_date = item.xpath('.//div[@class="property-data"]/div[4]/div[2]//text()').get()

            yield scrapy.Request(url, callback=self.populate_item,
                                 meta={
                                     "title": title,
                                     'external_id': external_id,
                                     "room_count": room_count,
                                     "square_meters": square_meters,
                                     "available_date": available_date,
                                 })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        object_information = response.xpath('//div[@class="property-details panel panel-default"]//ul//text()').getall()
        description = "".join(response.xpath(
            '//div[@class="property-description panel panel-default"]//div[@class="panel-body"]//text()').getall()).split(
            "Unsere aktuelle")[0]
        details = response.xpath('//div[@class="property-features panel panel-default"]//text()').getall()
        amenities = description + " ".join(details)
        deposit = None
        if "Kaution" in object_information:
            position = object_information.index("Kaution")
            deposit = object_information[position + 2]

        rent = None
        if "Kaltmiete" in object_information:
            position = object_information.index("Kaltmiete")
            rent = object_information[position + 2]

        location = None
        if "Adresse" in object_information:
            position = object_information.index("Adresse")
            location = object_information[position + 2]

        longitude, latitude = extract_location_from_address(location)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        utilities = None
        if "Betriebskosten brutto" in object_information:
            position = object_information.index("Betriebskosten brutto")
            utilities = object_information[position + 2]

        floor = None
        if "Etagen im Haus" in object_information:
            position = object_information.index("Etagen im Haus")
            floor = (object_information[position + 2])

        bathroom_count = None
        if "Badezimmer" in object_information:
            position = object_information.index("Badezimmer")
            bathroom_count = (object_information[position + 2])

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
        images = response.xpath('//div[@id="immomakler-galleria"]//img//@src').getall()
        landlord_name = response.xpath(
            '//div[@class="col-xs-12 col-sm-4"]//li[1]//span[@class="p-name fn"]//text()').get()
        landlord_email = response.xpath('//div[@class="dd col-sm-7 u-email value"]//text()').getall()[1]
        landlord_number = response.xpath('//div[@class="dd col-sm-7 p-tel value"]//text()').getall()[1]
        energy_label = response.xpath(
            '//div[@class="property-epass panel panel-default"]//li[last()]//div[2]//text()').get()
        # # MetaData
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", response.meta["external_id"])  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", response.meta["title"])  # String
        item_loader.add_value("description", remove_white_spaces(description))  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type", "apartment")  # String
        item_loader.add_value("square_meters", response.meta["square_meters"])  # Int
        item_loader.add_value("room_count", response.meta["room_count"])  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", extract_date(response.meta["available_date"]))  # String => date_format

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
        # item_loader.add_value("heating_cost", heating_cost)  # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1
        yield item_loader.load_item()
