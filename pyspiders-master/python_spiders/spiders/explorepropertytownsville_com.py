# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'explorepropertytownsville_com'
    external_source = "Explorepropertytownsville_PySpider_australia"
    execution_type='testing'
    country='australia'
    locale='en' 
    start_urls = ['https://clientapi.prolist.net.au/api/listings/map/search?embed=false']  # LEVEL 1
    
    payload = json.dumps({
        "SearchLevel": 3,
        "SearchGuid": "9b66b988-0144-43b3-a085-5a20621b01f0",
        "SearchGuids": [],
        "Page": 1,
        "PageSize": 65,
        "ExcludeConjunctionalListings": False,
        "IsHighlightListing": False,
        "IsFeatured": False,
        "IsPROListFeatured": False,
        "HaveInspections": False,
        "PropertyId": "",
        "Estate": "",
        "Categories": [
            1,
            4
        ],
        "Statuses": [
            0,
            1
        ],
        "PropertyTypes": [],
        "PropertyCategories": [],
        "PropertySubCategories": [],
        "MethodsOfSale": [
            1,
            2
        ],
        "AddressString": "",
        "LotNumber": "",
        "UnitNumber": "",
        "StreetNumber": "",
        "Suburbs": [],
        "BoundsNorth": -16.549177240625113,
        "BoundsEast": 152.63832651078985,
        "BoundsSouth": -25.856609558965403,
        "BoundsWest": 143.5196741670395,
        "OrderByStatements": [],
        "MinBedrooms": "",
        "MinBathrooms": "",
        "MinParking": "",
        "MinFloorArea": "",
        "MaxFloorArea": "",
        "MinLandArea": "0",
        "MaxLandArea": "0",
        "MinPrice": "",
        "MaxPrice": "",
        "MinSoldPrice": "",
        "MaxSoldPrice": "",
        "IsStrata": False,
        "IsFreehold": False,
        "IsTenanted": False,
        "IsVacant": False,
        "IsWholeBuilding": False
        })
    headers = {
        'authority': 'clientapi.prolist.net.au',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
        'sec-ch-ua': '"Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"',
        'x-prolist-client-website-id': 'DD1CD5B9-13D1-49EB-AA10-AAB6BE905933',
        'x-prolist-website-level': '3',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
        'content-type': 'application/json',
        'x-prolist-time-offset': '-180',
        'x-prolist-website-id': '9b66b988-0144-43b3-a085-5a20621b01f0',
        'sec-ch-ua-platform': '"Windows"',
        'accept': '*/*',
        'origin': 'https://explorepropertytownsville.com.au',
        'sec-fetch-site': 'cross-site',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://explorepropertytownsville.com.au/',
        'accept-language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4'
    }
    def start_requests(self):
        
        yield Request(
            url=self.start_urls[0],
            body=self.payload,
            headers=self.headers,
            method="POST",
            callback=self.parse
        )

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)["Unclustered"]
        
        for item in data:
            follow_url = f"https://explorepropertytownsville.com.au/listings/{item['Slug']}"
            if get_p_type_string(item["PropertyType"]):
                yield Request(
                    follow_url, 
                    callback=self.populate_item, 
                    meta={"property_type": get_p_type_string(item["PropertyType"]), "data": item}
                )
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        data = response.meta.get('data')
        item_loader.add_value("title", data["Header"])
        
        item_loader.add_value("external_id", str(data["PropertyId"]))
        
        address = data["Address"]["FullAddress"]
        
        city = data["Address"]["Suburb"]
        zipcode = f"{data['Address']['State']} {data['Address']['PostCode']}"
        
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        
        rent = data["Price"]["NumericPrice"]
        item_loader.add_value("rent", (int(float(rent))*4))
        item_loader.add_value("currency", "AUD")
        
        deposit = data["Price"]["BondPrice"]
        item_loader.add_value("deposit", deposit.replace("$","").replace(",",""))
        
        item_loader.add_value("room_count", data["Features"]["Bedrooms"])        
        item_loader.add_value("bathroom_count", data["Features"]["Bathrooms"])        
        
        parking = data["Features"]["Parking"]["Total"]
        if parking and parking>0:
            item_loader.add_value("parking", True)
            
        item_loader.add_value("floor", str(data["Features"]["NumberOfLevels"]))
        
        dishwasher = data["Features"]["HasDishwasher"]
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        balcony = data["Features"]["HasBalcony"]
        if balcony:
            item_loader.add_value("balcony", True)
        
        swimming_pool = data["Features"]["HasPoolAboveGround"]
        swimming_pool2 = data["Features"]["HasPoolInGround"]
        if swimming_pool or swimming_pool2:
            item_loader.add_value("swimming_pool", True)
        
        pets_allowed = data["Features"]["IsPetFriendly"]
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)
        
        furnished = data["Features"]["IsFurnished"]
        if furnished:
            item_loader.add_value("furnished", True)
        
        item_loader.add_value("description", data["WebDescription"])
        
        for i in data["Images"]:
            item_loader.add_value("images", i["Thumbs"]["Url"])
        
        import dateparser
        if data["DateAvailable"]:
            date_parsed = dateparser.parse(data["DateAvailable"], date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        item_loader.add_value("latitude", str(data["Address"]["Coordinates"]["Lat"]))
        item_loader.add_value("longitude", str(data["Address"]["Coordinates"]["Lon"]))
        
        if data['Agents']:
            landlord_name = f"{data['Agents'][0]['FirstName']} {data['Agents'][0]['LastName']}"
            landlord_phone = data["Agents"][0]["MobilePhone"]
            landlord_email = data["Agents"][0]["Email"]
            
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("landlord_email", landlord_email)
        else:
            item_loader.add_value("landlord_name", "Explore Property")
            item_loader.add_value("landlord_phone", "(07) 4750 4000")
                
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "flatshare" in p_type_string.lower():
        return "room"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "huis" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None