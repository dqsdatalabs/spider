# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import math

import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class immobilienpersicke(scrapy.Spider):
    name = "immobilienpersicke"
    start_urls = ['https://immobilien-persicke.de/miete']
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, method="GET", callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        urls= response.xpath('//h3[@class="property-title"]/a/@href').extract()
        for x in range(len(urls)):
            url = urls[x]
            yield scrapy.Request(url=url, callback=self.populate_item)



    # 3. SCRAPING level 3
    def populate_item(self, response):
        room_count = None
        bathroom_count = None
        floor = None
        parking = None
        elevator = None
        balcony = None
        washing_machine = None
        dishwasher = None
        utilities = None
        terrace = None
        furnished = None
        property_type = None
        energy_label = None
        deposit = None
        square_meters = None
        available = None
        heating_cost = None
        external_id = None
        item_loader = ListingLoader(response=response)
        title = "".join(response.xpath('//h1[@class="property-title"]/text()').extract()).strip()
        l2 = response.xpath('//*[@class="property-details panel panel-default"]/ul/li/div//descendant::text()').extract()
        l2 = [x.replace("  ", "").strip() for x in l2]
        l2 = [x for x in l2 if x]
        try:
            rent = int(float(l2[l2.index("Kaltmiete")+1].replace("EUR", "").replace(",", ".")))
        except:
            return
        external_id = l2[l2.index('Objekt ID') + 1]

        try:
            bathroom_count = int(float(l2[l2.index('Badezimmer') + 1].replace(",", ".")))
        except:
            pass
        try:
            square_meters = int(float(l2[l2.index('Wohnfläche ca.') + 1].replace(",", ".").replace(" m²", "")))
        except:
            square_meters = int(float(l2[l2.index('Gesamtfläche') + 1].replace(",", ".").replace(" m²", "")))
        try:
            deposit = int(float(l2[l2.index("Kaution")+1].replace(".","").replace("EUR", "").replace(",", ".").replace("€","")))
        except:
            pass
        try:
            room_count = int(float(l2[l2.index('Zimmer') + 1].replace(",", ".")))
        except:
            room_count = 1
        try:
            floor = l2[l2.index('Etage') + 1]
        except:
            pass

        try :
            utilities = int(float(l2[l2.index("Nebenkosten")+1].replace("EUR", "").replace(",", ".")))
        except:
            pass
        extras = "".join(response.xpath('//div[@class="property-features panel panel-default"]/ul/li/descendant::text()').extract())
        description = "".join(response.xpath('//div[@class="panel-body"]/p/descendant::text()').extract())
        description = description_cleaner(description)
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(description, extras, item_loader)
        images = response.xpath('//div[@id="immomakler-galleria"]/a/@href').extract()
        try:
            if l2.index("Balkone"):
                balcony = True
        except:
            pass

        l3=response.xpath('//div[@class="property-contact panel panel-default"]/div/ul/li/descendant::text()').extract()
        l3 = [x.replace("  ", "").strip() for x in l3]
        l3 = [x for x in l3 if x]
        try:
            landlord_name = l2[l2.index('Name') + 1]
        except:
            landlord_name = "Immobilien Persicke GmbH"
        try:
            landlord_number = l2[l2.index('Tel. Zentrale') + 1]
        except:
            landlord_number = "05741 / 3181-0"
        try:
            landlord_email = l2[l2.index('E-Mail Direkt') + 1]
        except:
            landlord_email = "info@immobilien-persicke.de"
        pro = l2[l2.index("Objekttypen")+1]
        if "wohnung" in pro.lower():
            property_type = "apartment"
        elif "haus" in pro.lower():
            property_type = "house"
        else:
            return
        address = l2[l2.index('Adresse') + 1].replace("\xa0", ",")
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, addres = extract_location_from_coordinates(longitude, latitude)
        # # MetaData
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
        item_loader.add_value("property_type",
                              property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available)  # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # # Monetary Status
        item_loader.add_value("rent", int(rent))  # Int
        item_loader.add_value("deposit", deposit)  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "EUR")  # String
        #
        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1
        yield item_loader.load_item()

