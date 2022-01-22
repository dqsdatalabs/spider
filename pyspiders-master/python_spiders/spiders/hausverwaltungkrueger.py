from io import StringIO

import requests
import scrapy
from ..helper import *
from ..loaders import ListingLoader
import json


class hausverwaltungkrueger(scrapy.Spider):
    name = 'hausverwaltungkrueger'
    allowed_domains = ['hausverwaltung-krueger.de']
    execution_type = 'testing'
    # start_urls1 = ['https://portal.fio.de/api/v1//portals/38c17b7c-3fc0-4756-809d-532b329f243f/estates']
    country = 'germany'
    locale = 'de'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        start_urls1 = ['https://portal.fio.de/api/v1//portals/38c17b7c-3fc0-4756-809d-532b329f243f/estates']

        head = {"Accept": "application/json, text/plain, */*"
            , 'Content-Type': 'application/json; charset=utf-8'
            ,
                "Referer": "https://portal.fio.de/api/v1/integration/Html/61373101-d4b6-4e00-a1f8-5ca060d14733/661a7d1a-9f18-4a2a-a931-dfd523a68014"
            , "Authorization": "ak 7015d319-b560-4aef-a2d1-15c685151a75"
            , "X-pUri": "https://www.hausverwaltung-krueger.de"
            , "Connection": "keep-alive"
            , "DNT": 1
            , "Host": "portal.fio.de"
            , "Origin": "https://portal.fio.de"
            , "Accept-Encoding": " gzip, deflate, br"}
        body ={"District":None,"EstateMetaCodeIds":[69],"EstateSubTypeIds":[],"FederalState":None,"GetTopEstates":False,"Latitude":"","Longitude":"","MaxPrice":None,"MaxRooms":"","MaxSpace":None,"MinPrice":None,"MinRooms":"","MinSpace":None,"OfferType":1,"OffererId":"","Radius":5,"Sort":3,"Street":None,"Town":"","Zip":None,"offererIdentifiers":[],"HasVirtualTour":False,"Page":1,"PageSize":"48","IsNewSearch":True,"IsResidentialProperty":False,"IsCommercialProperty":False,"IsInvestmentProperty":False,"NewDevelopment":False}
        body = json.dumps(body)
        # print(body)
        for url in start_urls1:
            yield scrapy.Request(url=url, method="POST", headers=head, body=body, callback=self.parse, dont_filter=True,
                                 meta={'dont_merge_cookies': True})

    def parse(self, response, **kwargs):
        global pos
        head = {"Accept": "application/json, text/plain, */*"
            , 'Content-Type': 'application/json; charset=utf-8'
            ,
                "Referer": "https://portal.fio.de/api/v1/integration/Html/61373101-d4b6-4e00-a1f8-5ca060d14733/661a7d1a-9f18-4a2a-a931-dfd523a68014"
            , "Authorization": "ak 7015d319-b560-4aef-a2d1-15c685151a75"
            , "X-pUri": "https://www.hausverwaltung-krueger.de"
            , "Connection": "keep-alive"
            , "DNT": 1
            , "Host": "portal.fio.de"
            , "Origin": "https://portal.fio.de"
            , "Accept-Encoding": " gzip, deflate, br"}
        body ={"District":None,"EstateMetaCodeIds":[69],"EstateSubTypeIds":[],"FederalState":None,"GetTopEstates":False,"Latitude":"","Longitude":"","MaxPrice":None,"MaxRooms":"","MaxSpace":None,"MinPrice":None,"MinRooms":"","MinSpace":None,"OfferType":1,"OffererId":"","Radius":5,"Sort":3,"Street":None,"Town":"","Zip":None,"offererIdentifiers":[],"HasVirtualTour":False,"Page":1,"PageSize":"48","IsNewSearch":True,"IsResidentialProperty":False,"IsCommercialProperty":False,"IsInvestmentProperty":False,"NewDevelopment":False}
        body = json.dumps(body)

        resp = json.loads(response.body)
        items= resp
        for x in items.get("EstateIdentifier"):
            url = f"https://portal.fio.de/api/v1//portals/38c17b7c-3fc0-4756-809d-532b329f243f/estates/{x}"
            yield scrapy.Request(url=url, method="GET", headers=head, body=body, callback=self.populate_item, dont_filter=True,meta={'dont_merge_cookies': True})
    def populate_item(self,response):
        parking=None
        floor=None
        bathroom_count=None
        pets_allowed=None
        terrace=None
        balcony=None
        swimming_pool=None
        resp = json.loads(response.body)
        item= resp
        item_loader = ListingLoader(response=response)
        # print(item)
        longitude = item.get("Longitude")
        latitude = item.get("Latitude")
        square_meters = int(item.get("LivingSpace"))
        try:
            floor = str(item.get("Floor"))
        except:
            pass
        try:
            bathroom_count = round(item.get("BathRooms"))
        except:
            pass
        room_count=round(item.get("RoomNumber"))
        deposit=int(item.get("Deposit"))
        energy_label=item.get("EnergyCertificateValueClass")
        description=item.get("EstateDescription")
        external_id = item.get("ExternalId")
        rent=item.get("RentCold")
        available=item.get("AvailabilityDate")
        utilities=int(item.get("SubsidaryCost"))
        title =item.get("EstateTitle")
        property_type="apartment"
        zipcode, city, address=extract_location_from_coordinates(longitude,latitude)
        pets_allowed1=item.get("PetsAllowed")
        if pets_allowed1:
            pets_allowed=True
        terrace1=item.get("Terrace")
        if terrace1:
            terrace=True
        balcony1=item.get("Balcony")
        if balcony1:
            balcony=True
        parkingspace=item.get("ParkingSpaceAll")
        if parkingspace>=1:
            parking=True
        try :
            landlord_name=item.get("ContactPerson").get("FirstName")+" "+item.get("ContactPerson").get("LastName")
        except:
            landlord_name="Krüger Haus- und Grundstücksverwaltung"
        try:
            landlord_number = item.get("ContactPerson").get("Telephone")
        except:
            landlord_number="0344756820"
        landlord_email="vermietung@hausverwaltung-krueger.de"
        images=[]
        for j in range (len(item.get("EstateMedia"))):
            token = item.get("EstateMedia")[j].get("Token")
            image="https://portal.fio.de/api/v1//media?token="+token
            images.append(image)
        # # MetaData
        item_loader.add_value("external_link",f"https://www.hausverwaltung-krueger.de/wohnungen-altenburg.html#/expose/{external_id}")  # String
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

        item_loader.add_value("available_date", available)  # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        # item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine)  # Boolean
        # item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # # Monetary Status
        item_loader.add_value("rent", int(rent))  # Int
        item_loader.add_value("deposit", deposit) # Int
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
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()

