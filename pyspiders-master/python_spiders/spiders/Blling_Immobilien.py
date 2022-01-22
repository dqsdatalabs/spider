# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
import json
from ..helper import  get_amenities


class BllingImmobilienSpider(scrapy.Spider):
    name = "Blling_Immobilien"
    start_urls = ['https://api.immowelt.com/auth/oauth/token']
    allowed_domains = ["api.immowelt.com", 'immowelt.de']
    country = 'germany'
    locale = 'de'
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
                "globalUserId": [841508]
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
        street = ' ' + \
            data["address"]["street"] if 'street' in data["address"] else ''
        district = ' ' + \
            data["address"]["district"] if 'district' in data["address"] else ''
        latitude = str(data["address"]["latitude"]
                       ) if 'latitude' in data["address"] else None
        longitude = str(data["address"]["longitude"]
                        ) if 'longitude' in data["address"] else None
        json_data = json.loads(response.css(
            "script[type='application/json']::text").get().replace("&q;", '"'))['expose/' + external_id.lower()]
        latitude = json_data['EstateMapData']['LocationCoordinates']['Latitude'] if latitude == None else latitude
        longitude = json_data['EstateMapData']['LocationCoordinates']['Longitude'] if longitude == None else longitude
        prices = json_data['Price']['DataTable']
        utilities = None
        for item in prices:
            if 'PRICE_ADDITIONALCOSTS' in item['Key']:
                utilities = item['NumberValue']
            if 'PRICE_HEATINGCOSTS' in item['Key']:
                heating_cost = item['NumberValue']


        address = zipcode + ' ' + city + street + district

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
        deposit = json_data['Price']['AdditionalInformation']['Deposit']    
        numbers = {
            'eins': 1,
            'zwei': 2,
            'drei': 3,
            'vier': 4,
            'fünf': 5,
            'sechs': 6,
            'sieben': 7,
            'acht': 8,
            'neun': 9,
            'zehn': 10
        }
        if 'StringValue' in deposit.keys():
            deposit = numbers[deposit['StringValue'].lower().split(' ')[0]] * rent
        else:
            deposit = deposit['NumberValue']
            

        if rent == None:
            return
        
        energy_label = json_data['EnergyPasses'][0]['Data'][0]['Class'][-1]
        amenties = ''
        for item in json_data['EquipmentAreas']:
            for equipment in item['Equipments']:
                if equipment['Key'] == 'FLOOR':
                    floor =  equipment['Value'][0] if equipment['Value'][0].isnumeric() else '0'           
                amenties += equipment['Value']
        bathroom_count = 1
        description = ''
        descriptions = json_data['Texts']
        for text in descriptions:
            description += text['Content'] + '\n'

        description = description[:description.find("&q")]
        landlord_name = data['broker']['companyName']
        landlord_number = json_data['Offerer']['contactData']['phone']
        landlord_email = 'mail@buelling-immobilien.de'
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
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        item_loader.add_value("floor", floor) # String
        # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int
        # String => date_format
        item_loader.add_value("available_date", available_date)
        # #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # #item_loader.add_value("furnished", furnished) # Boolean
        # item_loader.add_value("parking", parking)  # Boolean
        # #item_loader.add_value("elevator", elevator) # Boolean
        # #item_loader.add_value("balcony", balcony) # Boolean
        # #item_loader.add_value("terrace", terrace) # Boolean
        # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # #item_loader.add_value("washing_machine", washing_machine) # Boolean
        # #item_loader.add_value("dishwasher", dishwasher) # Boolean
        get_amenities(description, amenties, item_loader)

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)  # Int

        item_loader.add_value("currency", currency)  # String
        item_loader.add_value("heating_cost", heating_cost)  # Int
        item_loader.add_value("energy_label", energy_label) # String

        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1
        yield item_loader.load_item()
