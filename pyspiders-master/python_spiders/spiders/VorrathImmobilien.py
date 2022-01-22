# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_coordinates
import re


class VorrathimmobilienSpider(scrapy.Spider):
    name = "VorrathImmobilien"
    start_urls = ['https://api.immowelt.com/auth/oauth/token']
    allowed_domains = ["api.immowelt.com", "immowelt.de", "vorrath.org"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        header = {
            "authorization": 'Basic ZXhwb3NlLXVpOmRreGI0WTJjN2hKTiReajlAVEs='
        }
        body = {
            "grant_type": "client_credentials"
        }
        for url in self.start_urls:
            yield scrapy.FormRequest(url=url, headers=header, formdata=body, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        access_token = json.loads(response.body)["access_token"]
        token = 'Bearer ' + access_token
        body = {
            "general": {
                "globalUserId": [28012]
            },
            "pagesize": 10000
        }
        yield scrapy.Request('https://api.immowelt.com/estatesearch/EstateSearch/v1/Search', callback=self.populate_items, headers={"Authorization": token, "Content-Type": "application/json"}, method='POST', body=json.dumps(body))

    # 3. SCRAPING level 3
    def populate_items(self, response):
        listings = json.loads(response.body)["items"]
        for listing in listings:
            yield scrapy.Request('https://www.immowelt.de/expose/' + listing['onlineId'], callback=self.populate_item, meta={**listing})
            
            
    # 4. SCRAPING level 4
    def populate_item(self, response):
        data = response.meta
        if not 'type' in data["price"] or 'rent' not in data["price"]["type"] or "value" not in data["price"]:
            return
        rent = int(data['price']["value"])
        external_id = data["onlineId"]
        title = data["name"]
        property_types = {'WOHNUNG': 'apartment', 'HAUS': 'house'}
        if data['estateType'] not in property_types:
            return
        square_meters = None
        property_type = property_types[data['estateType']]
        if 'areas' in data:
            if 'livingArea' in data['areas']:
                if 'value' in data['areas']['livingArea']:
                    square_meters = int(data['areas']["livingArea"]["value"])
        room_count = int(data['rooms'])
        if room_count == 0:
            room_count = 1
        available_date = None
        equipments = []
        if 'equipment' in data:
            equipments = data['equipment']
        parking = balcony = elevator = terrace = washing_machine = None
        heating_cost = None
        for equipment in equipments:
            if "stellplatz" in equipment["value"].lower() or "garage" in equipment["value"].lower() or "parkhaus" in equipment["value"].lower() or "tiefgarage" in equipment["value"].lower() or 'parking' in equipment["value"].lower():
                parking = True
            if 'balkon' in equipment["value"].lower():
                balcony = True
            if 'aufzug' in equipment["value"].lower():
                elevator = True
            if 'terrasse' in equipment["value"].lower():
                terrace = True
            if 'waschmaschine' in equipment["value"].lower():
                washing_machine = True

        images = data["images"]
        for inx, img in enumerate(images):
            images[inx] = img["large"]["uri"]

        currency = data['price']["currency"]
        jsn = json.loads(response.css("script[type='application/json']::text").get().replace("&q;", '"'))
        utilities = None
        deposit = None
        prices = jsn[f"expose/{external_id.lower()}"]["Price"]["DataTable"]
        for price in prices:
            if price['Label'] == 'Nebenkosten':
                utilities = price['NumberValue']
            if price['Label'] == 'Kaution':
                deposit = int(re.findall(r'\d+', price['StringValue'])[0]) * rent
        if "AdditionalInformation" in jsn[f"expose/{external_id.lower()}"]["Price"]:
            if "Deposit" in jsn[f"expose/{external_id.lower()}"]["Price"]["AdditionalInformation"]:
                deposit = jsn[f"expose/{external_id.lower()}"]["Price"]["AdditionalInformation"]["Deposit"]["StringValue"]
                deposit = int(re.findall(r'\d+', deposit)[0]) * rent
        floor = None
        equipments_area = jsn[f"expose/{external_id.lower()}"]["EquipmentAreas"]
        for area in equipments_area:
            for equipement in area['Equipments']: 
                if equipement['Key'] == 'FLOOR':
                    if len(re.findall(r'\d+', equipement['Value'])) > 0:
                        floor = str(re.findall(r'\d+', equipement['Value'])[0])

        description = jsn[f"expose/{external_id.lower()}"]["Texts"][0]["Content"]
        
        lower_description = description.lower() + " " + title.lower()
        if "stellplatz" in lower_description or "garage" in lower_description or "parkhaus" in lower_description or "tiefgarage" in lower_description:
            parking = True
        if 'balkon' in lower_description:
            balcony = True
        if 'aufzug' in lower_description:
            elevator = True
        if 'terrasse' in lower_description:
            terrace = True
        if 'waschmaschine' in lower_description:
            washing_machine = True
        
        contact_data = jsn[f"expose/{external_id.lower()}"]["Offerer"]["contactData"]
        landlord_name = contact_data["firstName"] + ' ' + contact_data["lastName"]
        landlord_number = contact_data["phone"] or contact_data["mobile"]
        landlord_email = None
        
        latitude = jsn[f"expose/{external_id.lower()}"]["EstateMapData"]["LocationCoordinates"]["Latitude"]
        longitude = jsn[f"expose/{external_id.lower()}"]["EstateMapData"]["LocationCoordinates"]["Longitude"]
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        
        item_loader = ListingLoader(response=response)

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String
        item_loader.add_value("position", self.position) # Int

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String
        
        self.position += 1
        yield item_loader.load_item()