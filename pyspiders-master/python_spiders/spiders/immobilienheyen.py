# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import json

import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class WellgroundedrealestateSpider(scrapy.Spider):
    name = "immobilienheyen"
    start_urls = ["https://immobilien-heyen.de/flow.php?getEntries=true&schema=flat_rent"]
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
    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        global pos
        resp = json.loads(response.body)
        items = resp
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
        swimming_pool = None
        external_id = None
        pets_allowed = None
        heating_cost = None
        for item in items.get("entries"):
            item_loader = ListingLoader(response=response)
            longitude = item.get("longitude").get("values")[0]
            latitude = item.get("latitude").get("values")[0]
            try:
                utilities = item.get("additionalexpenses").get("values")[0]
            except:
                pass
            try:
                bathroom_count = item.get("numberbathrooms").get("values")[0]
            except:
                pass
            try:
                rent = item.get("rent").get("values")[0]
            except:
                return
            try:
                square_meters = item.get("livingarea").get("values")[0]
            except:
                pass
            try:
                room_count = item.get("rooms").get("values")[0]
            except:
                pass
            try:
                deposit = item.get("securitydeposit").get("values")[0]
            except:
                pass

            try:
                energy_label = item.get("energy_certificate_availability").get("values")[0]
            except:
                pass
            title = "".join(item.get("headline").get("values"))
            external_id = "".join(item.get("identifier").get("values"))

            description = "".join(item.get("textEstate").get("values"))

            try:
                extras = "".join(item.get("textEnvironment").get("values"))
            except:
                extras = ""
            images = []
            pics = item.get("onlineImage").get("values")
            for x in pics:
                images.append(x.get("uri"))
            property_type = "apartment"
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            landlord_number = "04961/985990"
            landlord_email = "info@immobilien-heyen.de"
            landlord_name = "Immobilien Heyen"
            description = description_cleaner(description)
            pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(
                description, extras, item_loader)
            try:
                if item.get("balconyavailable").get("values"):
                    balcony = True
            except:
                pass
            try:
                if item.get("petsallowed").get("values"):
                    pets_allowed = True
            except:
                pass
            if "stellplatz" in extras.lower():
                parking=True
            id=item.get("id")
            # # MetaData
            # # MetaData
            item_loader.add_value("external_link", f"https://immobilien-heyen.de/expose_view.php?id={id}")  # String
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
            item_loader.add_value("property_type",property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            # item_loader.add_value("available_date", available)  # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
            item_loader.add_value("furnished", furnished)  # Boolean
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
            # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

            # # Monetary Status
            item_loader.add_value("rent", int(rent))  # Int
            item_loader.add_value("deposit", deposit)  # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "EUR")  # String
            #
            # item_loader.add_value("water_cost", water_cost) # Int
            item_loader.add_value("heating_cost", heating_cost)  # Int

            item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name)  # String
            item_loader.add_value("landlord_phone", landlord_number)  # String
            item_loader.add_value("landlord_email", landlord_email)  # String

            self.position += 1
            yield item_loader.load_item()