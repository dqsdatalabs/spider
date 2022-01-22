#Author : Ahmed Omran

import requests
import scrapy
from ..helper import *
from ..loaders import ListingLoader
import json



class morrisonsellsrealestate(scrapy.Spider):
    name = 'morrisonsellsrealestate'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    position=1

    def start_requests(self):

        start_urls = [
            'https://3pvidx.torontomls.net/idx/services/treb/Idx.ashx?callback=jsonp1638181836435&criteria={"Name":"Unnamed+IDX+Search","Conditions":[{"Name":"class","Predicates":[{"Opcode":"=","Value":"Free"}],"Type":"String"},{"Name":"latitude","Predicates":[{"Opcode":">=","Value":43.20537787786828},{"Opcode":"<=","Value":44.09961995791981}],"Type":"String"},{"Name":"longitude","Predicates":[{"Opcode":">=","Value":-79.99518789296873},{"Opcode":"<=","Value":-78.77021210703124}],"Type":"String"}],"Created":"\/Date(1638181898470)\/","CriteriaID":0,"Modified":"\/Date(1638181898470)\/"}&sig=47163e1a0f86c674834a5a7275c85f9c&method=listings:search&api_key=4d842b8e9b299f7c53d7f8181099f0f8&ref=62706b88c48fcde403a267e9f32cffab7d91d29ae10c87325da2ed2614e883e9a4e202b2&_=1638181898488'
                     ]
        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):

        resp = json.loads(response.body[1855:].replace(b"(",b"").replace(b")",b"").replace(b";",b""))
        items= resp
        for i,item in enumerate (items.get("rows")):
            if item[10] == "Lease":
                item_loader = ListingLoader(response=response)
                # print(item)
                longitude = item[2]
                latitude = item[1]
                rent = item[14]
                landlord_name = item[-3]
                landlord_number = item[-2]
                room_count = item[17]
                bathroom_count = item[18]
                external_id = item[0]
                title = "".join(item[12] + ", " + item[13])
                zipcode, city, address=extract_location_from_coordinates(longitude,latitude)
                property_type="apartment"
                # # MetaData
                item_loader.add_value("external_link","https://morrisonsellsrealestate.com/search-listings/"+f"#{i}")  # String
                item_loader.add_value("external_source", self.external_source)  # String
                item_loader.add_value("external_id", external_id)  # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", title)  # String
                # item_loader.add_value("description", description)  # String
                # # Property Details
                item_loader.add_value("city", city)  # String
                item_loader.add_value("zipcode", zipcode)  # String
                item_loader.add_value("address", address)  # String
                item_loader.add_value("latitude", str(latitude))  # String
                item_loader.add_value("longitude", str(longitude))  # String
                # item_loader.add_value("floor", floor)  # String
                item_loader.add_value("property_type",property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
                # item_loader.add_value("square_meters", square_meters)  # Int
                item_loader.add_value("room_count", room_count)  # Int
                item_loader.add_value("bathroom_count", bathroom_count)  # Int

                # item_loader.add_value("available_date", available)  # String => date_format

                # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                # item_loader.add_value("furnished", furnished) # Boolean
                # item_loader.add_value("parking", parking)  # Boolean
                # item_loader.add_value("elevator", elevator)  # Boolean
                # item_loader.add_value("balcony", balcony)  # Boolean
                # item_loader.add_value("terrace", terrace)  # Boolean
                # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                # item_loader.add_value("washing_machine", washing_machine)  # Boolean
                # item_loader.add_value("dishwasher", dishwasher)  # Boolean

                # # Images
                # item_loader.add_value("images", images)  # Array
                # item_loader.add_value("external_images_count", len(images))  # Int
                # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

                # # Monetary Status
                item_loader.add_value("rent", int(rent))  # Int
                # item_loader.add_value("deposit", deposit) # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                # item_loader.add_value("utilities", utilities)  # Int
                item_loader.add_value("currency", "CAD")  # String
                #
                # item_loader.add_value("water_cost", water_cost) # Int
                # item_loader.add_value("heating_cost", heating_cost) # Int

                # item_loader.add_value("energy_label", energy_label)  # String

                # # LandLord Details
                item_loader.add_value("landlord_name", landlord_name)  # String
                item_loader.add_value("landlord_phone", landlord_number)  # String
                # item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()

