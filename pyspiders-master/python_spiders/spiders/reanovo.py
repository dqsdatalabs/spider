# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import json
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class reanovo(scrapy.Spider):
    name = "reanovo"
    start_urls = ['https://reanovo.everreal.co/api/prism/public/expose?take=37&t=1641169321495']
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
        global pos
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
        square_meters=None
        swimming_pool=None
        external_id = None
        pets_allowed=None
        heating_cost=None
        resp = json.loads(response.body)
        items = resp
        for i, item in enumerate(items):
            if item.get("type") == "RENT_APARTMENT":
                item_loader = ListingLoader(response=response)
                id=item.get("id")
                title=item.get("title")
                property_type="apartment"
                longitude = item.get("property").get("address").get("location").get("lng")
                latitude = item.get("property").get("address").get("location").get("lat")
                rent = int(item.get("contractDetails").get("rent"))
                deposit=int(item.get("contractDetails").get("deposit"))
                try :
                    heating_cost=int(item.get("contractDetails").get("heatingCosts"))
                except :
                    pass
                utilities=int(item.get("contractDetails").get("utilityCosts"))
                pets=item.get("contractDetails").get("petsAllowed")
                pics=item.get("pictures")
                floor_plan_images=[]
                images=[]
                floorr=item.get("floorplans")
                for y in floorr :
                    floor_plan_images.append("https://resources.everreal.co/"+y.get("resourcePath"))
                square_meters=int(item.get("unit").get("livingSurface"))
                for x in pics :
                    images.append("https://resources.everreal.co/"+x.get("resourcePath"))
                if pets == "DEPENDS" :
                    pets_allowed=True
                room_count=int(item.get("unit").get("rooms").get("rooms"))
                bathroom_count=int(item.get("unit").get("rooms").get("bathrooms"))
                try :
                    floor=str(item.get("unit").get("floorNo"))
                except:
                    pass
                zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
                landlord_number="+49 69 3400-1152"
                landlord_email="info@reanovo.de"
                landlord_name=item.get("listingResponsible").get("fullName")
                energy_label=item.get("amenities").get("energyEfficiencyClass")
                amen=item.get("amenities").get("amenitiesIncluded")
                if "ELEVATOR" in amen :
                    elevator=True
                if "BALCONY_OR_TERRACE" in amen :
                    balcony=True
                    terrace=True
                if item.get("amenities").get("parking"):
                    parking=True
                description="".join(item.get("descriptions").values())
                # # MetaData
                item_loader.add_value("external_link", f"https://reanovo.everreal.co/app/public/expose/{id}")  # String
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

                item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking)  # Boolean
                item_loader.add_value("elevator", elevator)  # Boolean
                item_loader.add_value("balcony", balcony)  # Boolean
                item_loader.add_value("terrace", terrace)  # Boolean
                item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                item_loader.add_value("washing_machine", washing_machine)  # Boolean
                item_loader.add_value("dishwasher", dishwasher)  # Boolean

                # # Images
                item_loader.add_value("images", images)  # Array
                # item_loader.add_value("external_images_count", len(images))  # Int
                item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

                # # Monetary Status
                item_loader.add_value("rent", int(rent))  # Int
                item_loader.add_value("deposit", deposit) # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                item_loader.add_value("utilities", utilities)  # Int
                item_loader.add_value("currency", "EUR")  # String
                #
                # item_loader.add_value("water_cost", water_cost) # Int
                item_loader.add_value("heating_cost", heating_cost) # Int

                item_loader.add_value("energy_label", energy_label)  # String

                # # LandLord Details
                item_loader.add_value("landlord_name", landlord_name)  # String
                item_loader.add_value("landlord_phone", landlord_number)  # String
                item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()

