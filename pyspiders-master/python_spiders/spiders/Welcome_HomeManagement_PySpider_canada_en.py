# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
from datetime import datetime
import json


class WelcomeHomemanagementPyspiderCanadaEnSpider(scrapy.Spider):
    name = "Welcome_HomeManagement"
    start_urls = ['https://app.tenantturner.com/listings-json/2616']
    allowed_domains = ["welcomehomepropertymanagement.ca", "app.tenantturner.com"]
    country = 'canada' 
    locale = 'en'
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
        listings = json.loads(response.body)
        for listing in listings:
            yield scrapy.Request(listing['btnUrl'], callback=self.populate_item, meta={**listing})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        data = response.meta
        external_id = data["id"]
        title = data["title"]
        description = data["description"].replace("=======================================================<br/>Property Professionally Managed By Welcome Home Management.<br/><br/>Interested in putting Welcome Home Management to work for you? Learn more at https://welcomehomepropertymanagement.ca/experience/<br/>=======================================================", "")
        city = data["city"]
        zipcode = data["zip"]
        address = data["address"]
        latitude = data["latitude"]
        longitude = data["longitude"]
        property_types = {"Duplex": "apartment", "Apartment Unit": "apartment", "Single Family": "house", "Townhouse": "house"}
        if data["propertyType"] == None:
            return
        property_type = property_types[data["propertyType"]]
        if data["beds"] == "TBD":
            room_count = 1
        else:
            room_count = int(data["beds"])
        if data["baths"]:
            bathroom_count = int(data["baths"][0])
        available_date = None
        if data["dateAvailable"] != None or data["dateAvailable"] != "":
            available_date = datetime.now().strftime("%Y-%m-%d") if data["dateAvailable"].lower() == 'now' else  data["dateAvailable"][:data["dateAvailable"].find(" ")]
        pets_allowed = False
        if data["acceptPets"] != "" or data["acceptPets"] != None:
            pets_allowed = True
        parking = False
        if description.lower().find("parking") != -1:
            parking = True
        balcony = False
        if description.lower().find("balcony") != -1:
            balcony = True
        washing_machine = False
        if description.lower().find("laundry") != -1:
            washing_machine = True
        dishwasher = False
        if description.lower().find("dishwasher") != -1:
            dishwasher = True
        description += response.css("table.pre-qualify__details-table.table span::text").getall()[0]
        images = []
        images.append(data["photo"])
        for img in response.css("div.pre-qualify__slider.royalSlider.rsDefault img::attr(src)").getall():
            images.append(img) 
        sq = response.css("h2.pre-qualify__rental-title::text").get()
        square_meters = None
        if 'sqft' in sq:
            print(int(sq[sq.rfind('/')+2:].split(" ")[0]))
            square_meters = int(int(sq[sq.rfind('/')+2:].split(" ")[0]) / 10.764)

        rent = data["rentAmount"]
        table = response.css("table.pre-qualify__details-table.table td::text").getall()
        deposit = None
        for row in table:
            if row.strip().lower().find("$") != -1:
                deposit = row[1:].replace(",", "")
        item_loader = ListingLoader(response=response)

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "CAD") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "Welcome Home Managemen") # String
        item_loader.add_value("landlord_phone", "1-855-375-3300") # String
        # item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
