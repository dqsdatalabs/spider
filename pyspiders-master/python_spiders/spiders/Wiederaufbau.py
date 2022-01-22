# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
import json



class WiederaufbauSpider(scrapy.Spider):
    name = "Wiederaufbau"
    start_urls = ['https://api.immowelt.com/auth/oauth/token']
    allowed_domains = ["api.immowelt.com", 'immowelt.de', 'wiederaufbau.de']
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1
    address = {}

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
                "globalUserId": [1420002]
            },
            "pagesize": 10000
        }
        yield scrapy.Request('https://api.immowelt.com/estatesearch/EstateSearch/v1/Search', callback=self.populate_items, headers={"Authorization": token, "Content-Type": "application/json"}, method='POST', body=json.dumps(body))

    # 3. SCRAPING level 3
    def populate_items(self, response):
        listings = json.loads(response.body)["items"]
        for listing in listings:
            yield scrapy.Request('https://www.immowelt.de/expose/' + listing['onlineId'], callback=self.populate_item, meta={**listing})
    # 3. SCRAPING level 4

    def populate_item(self, response):
        data = response.meta
        external_id = data["onlineId"]
        title = data["name"]
        city = data["address"]["city"]
        zipcode = data["address"]["zip"]
        street = ' ' + data["address"]["street"] if 'street' in data["address"] else ''
        district = ' ' + data["address"]["district"] if 'district' in data["address"] else ''
        latitude = str(data["address"]["latitude"])  if 'latitude' in data["address"] else None
        longitude = str(data["address"]["longitude"]) if 'longitude' in data["address"] else None
        json_data = json.loads(response.css("script[type='application/json']::text").get().replace("&q;",'"'))['expose/' + external_id.lower()]
        latitude = json_data['EstateMapData']['LocationCoordinates']['Latitude'] if latitude == None else latitude
        longitude = json_data['EstateMapData']['LocationCoordinates']['Longitude'] if longitude == None else longitude
        prices = json_data['Price']['DataTable']
        utilities = None
        for item in prices:
            if 'PRICE_ADDITIONALCOSTS' in item['Key']:
                utilities = item['NumberValue']



        
        address = zipcode + ' ' + city + street  + district


        property_types = {'WOHNUNG': 'apartment', 'HAUS': 'house'}
        if data['estateType'] not in property_types:
            return
        square_meters = None
        property_type = property_types[data['estateType']]
        if 'areas' in data:
            if 'livingArea' in data['areas']:
                if 'value' in data['areas']['livingArea']:
                    square_meters = int(data['areas']["livingArea"]["value"])
        room_count = data['rooms']
        if room_count == 0:
            room_count = 1
        available_date = data['creationDate']
        equipments = []
        if 'equipment' in data:
            equipments = data['equipment']
        parking = False
        heating_cost = False
        for equipment in equipments:
            if equipment["value"] == "GARAGE":
                parking = True
            if equipment["value"] == "ZENTRALHEIZUNG":
                heating_cost = True

        images = data["images"]
        for inx, img in enumerate(images):
            images[inx] = img["large"]["uri"]

        currency = data['price']["currency"]
        rent = None
        if 'price' in data:
            if 'value' in data['price']:
                if data['price']['value'] == None:
                    return
                rent = int(data['price']["value"])
            if 'type' in data['price']:
                if 'rent' not in data['price']["type"]:
                    return
        jsn = response.css("script[type='application/json']::text").get()
        json_data = json.loads(response.css(
            "script[type='application/json']::text").get().replace("&q;", '"'))['expose/' + external_id.lower()]
        prices = json_data['Price']['DataTable']
        utilities = None
        for item in prices:
            if 'PRICE_ADDITIONALCOSTS' in item['Key']:
                utilities = item['NumberValue']
            if 'PRICE_HEATINGCOSTS' in item['Key']:
                heating_cost = item['NumberValue']

        deposit = None
        deposit_index = jsn.find("Kaution")
        if deposit_index != -1:
            start = deposit_index + len("Kaution&q;,&q;NumberValue&q;:")
            dep = jsn[start:start+10]
            deposit = dep[:dep.find(",")]

        start = jsn.find("Content&q;:&q;") + 14
        description = jsn[start:]
        description = description[:description.find("&q")]
        landlord_name = data['broker']['companyName']
        landlord_number = '036 651 3522'
        landlord_email = 'info@rtl-immobilien.com'
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value(
            "external_source", self.external_source)  # String
        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String will get it
        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", latitude)  # String
        item_loader.add_value("longitude", longitude)  # String
        # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        # String => date_format
        item_loader.add_value("available_date", available_date)
        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)  # will get it
        item_loader.add_value("utilities", utilities) # Int

        item_loader.add_value("currency", currency)  # String
        item_loader.add_value("heating_cost", heating_cost)  # Int
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1
        yield item_loader.load_item()
