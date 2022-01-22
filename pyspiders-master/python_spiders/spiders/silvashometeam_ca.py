# -*- coding: utf-8 -*-
# Author: Ahmed Atef
import scrapy
from scrapy import Request, FormRequest
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from scrapy.utils.url import add_http_if_no_scheme

from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs
import json
from w3lib.html import remove_tags

class SilvashometeamSpider(scrapy.Spider):
        
    name = 'silvashometeam_ca'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['silvashometeam.com']    
    

    position = 1
    properties_url = "https://api.mapsearch.vps-private.net/properties"
    accessToken = None
    body_data = {
        "query": {
            "PropertyType": {
                "$in": [
                    "Townhouse",
                    "House",
                    "Duplex",
                    "Triplex",
                    "Fourplex",
                    "Garden Home",
                    "Mobile Home",
                    "Manufactured Home/Mobile",
                    "Special Purpose",
                    "Residential Commercial Mix",
                    "Manufactured Home",
                    "Residential Detached",
                    "Residential Farm",
                    "Co-ownership Apt",
                    "Residential",
                    "Attached/Row/",
                    "Att/Row/Townhouse",
                    "Row / Townhouse",
                    "Att/Row/Twnhouse",
                    "House/Single Family",
                    "Semi-Detached",
                    "Detached",
                    "Row House (Non-Strata)",
                    "pre construction condos",
                    "Condominium",
                    "Single Family",
                    "Multi-Family",
                    "Multiplex",
                    "Two Apartment House",
                    "1/2 Duplex",
                    "Semi-Det Condo",
                    "House with Acreage",
                    "Rural Resid",
                    "2-Apartment",
                    "Det Condo",
                    "Time Shared",
                    "3 Units",
                    "Cottage/Rec Properties",
                    "Cottage / Rec",
                    "Mobile/Mini",
                    "APTU",
                    "RUR",
                    "Commercial/Retail",
                    "Time Share",
                    "4 units",
                    "Shared Room",
                    "Flex Facility",
                    "Licensed Student Dwelling",
                    "Leasehold Condo",
                    "RES",
                    "Residential Commercial Mix",
                    "Vacant Land Condo",
                    "Manufacturing",
                    "Phased Condo",
                    "Lower Level",
                    "Modular",
                    "Mobile/Trailer",
                    "Co-op Apt",
                    "Store W/Apt/Offc",
                    "Det W/Com Elements",
                    "Cottage",
                    "5 - 8 Unit",
                    "Acreages",
                    "CON",
                    "waterfront",
                    "Manufactured",
                    "Recreational",
                    "Room",
                    "Manufactured with Land",
                    "Farm",
                    "Link",
                    "Upper Level",
                    "Bungalow",
                    "Condo Apt",
                    "pre construction condos",
                    "Leasehold Condo",
                    "Co-ownership Apt",
                    "Vacant Land Condo",
                    "Apartment/Condo",
                    "Condominiums",
                    "Apartment Unit",
                    "Co-op Apt",
                    "Store W/Apt/Offc",
                    "Locker",
                    "Condominium",
                    "Det W/Com Elements",
                    "Semi-Det Condo",
                    "Comm Element Condo",
                    "Apartment",
                    "Two Apartment House",
                    "2-Apartment",
                    "2-Apartment",
                    "Garden Home",
                    "Mobile Home",
                    "Mobile/Trailer",
                    "Manufactured Home/Mobile",
                    "Mobile/Mini",
                    "Manufactured Home",
                    "Manufactured",
                    "Manufactured with Land",
                    "Special Purpose",
                    "Residential Commercial Mix",
                    "Condo",
                    "Condo Townhouse",
                    "Mobile Home",
                    "Manufactured Home",
                    "Manufactured Home/Mobile",
                    "Special Purpose",
                    "Residential Commercial Mix",
                    "Other",
                    "Commercial Apartment",
                    "Cottage/Rec Properties",
                    "Cottage / Rec",
                    "Mobile/Mini",
                    "Recreational",
                    "waterfront",
                    "Cottage",
                    "Mobile/Trailer",
                    "Time Shared",
                    "Time Share",
                    "Row / Townhouse",
                    "Apartment",
                    "Garden Home",
                    "Mobile Home",
                    "Manufactured Home/Mobile",
                    "Special Purpose",
                    "Residential Commercial Mix",
                    "Other",
                    "Manufactured Home",
                    "Multi Family",
                    "Duplex",
                    "Triplex",
                    "Fourplex"
                ]
            },
            "coordinates": {
                "$geoWithin": {
                    "$box": [
                        [
                            -101.74736557640401,
                            41.64959514965462
                        ],
                        [
                            -68.04131088890401,
                            48.50175033616955
                        ]
                    ]
                }
            },
            "listingType": "Rent",
            "searchType": "residential"
        },
        "limit": 5000,
        "skip": 0,
        "soldData": False
    }
    
    def start_requests(self):
        start_urls = "https://api.mapsearch.vps-private.net/auth/anonymous"
        
        body_data = {
            "ttl": 86400
        }

        yield Request(  url = start_urls, 
                        method="post",
                        body = json.dumps(body_data), 
                        callback=self.parse_to_get_accessToken, 
                        dont_filter=True
                      )
    def parse_to_get_accessToken(self, response):
        
        jsonResponse = response.json()
        accessToken = jsonResponse["accessToken"]
        self.accessToken = accessToken
        
        yield Request(  url = self.properties_url, 
                method="SEARCH",
                headers={"Access-Token":accessToken, "access_token":accessToken},
                body = json.dumps(self.body_data), 
                callback=self.parse_all_data, 
                dont_filter=True
            )
              


    def parse_all_data(self, response):

        
        jsonResponse = response.json()
        

        for index, card in enumerate(jsonResponse):

            position = self.position
            card_url = card["url"]
                  
            dataUsage = {
                "position": position,
            }
            
            
            SilvashometeamSpider.position += 1
            yield Request(  card_url, 
                            method="get",
                            headers={"Access-Token":self.accessToken, "access_token":self.accessToken},
                            callback=self.parseApartment, 
                            dont_filter=True, 
                            meta=dataUsage
                            )
            
        
        if len(jsonResponse) > 0 :
            self.body_data["skip"] = self.body_data["skip"] + self.body_data["limit"]
            yield Request(  url = self.properties_url, 
                    method="SEARCH",
                    headers={"Access-Token":self.accessToken, "access_token":self.accessToken},
                    body = json.dumps(self.body_data), 
                    callback=self.parse_all_data, 
                    dont_filter=True
                )



    def parseApartment(self, response):

        jsonResponse = response.json()
        
        external_link = f"https://www.silvashometeam.com{jsonResponse['seoURL']}"
        external_id = f'{jsonResponse["id"]}-{jsonResponse["PID"]}-{jsonResponse["ListingID"]}'
        property_type = "house"
            


        if "LotSquareFootage" in jsonResponse:
            square_meters = jsonResponse["LotSquareFootage"]
            if square_meters:
                square_meters = sq_feet_to_meters(square_meters)
        else:
            square_meters = None
            

        if "Bedrooms" in jsonResponse["Details"]:
            room_count =  jsonResponse["Details"]["Bedrooms"]
        else:
            room_count = None
        
        if "Bathrooms" in jsonResponse["Details"]:
            bathroom_count = jsonResponse["Details"]["Bathrooms"]
        else:
            bathroom_count = None

        
        rent = jsonResponse["OriginalPrice"]
        if rent and rent > 10:
            rent = int(rent)
            pass
        else:
            rent = None
            
        currency = "CAD"

        
        title = f'{jsonResponse["Address"]}, {jsonResponse["City"]}, {jsonResponse["PostalCode"]}, {jsonResponse["Province"]}, {jsonResponse["Country"]}, {jsonResponse["Source"]}'
        
        address = f'{jsonResponse["Address"]}, {jsonResponse["City"]}, {jsonResponse["PostalCode"]}, {jsonResponse["Province"]}, {jsonResponse["Country"]}, {jsonResponse["Source"]}'
        
        city = jsonResponse["City"]
        latitude = str(jsonResponse["Latitude"])
        longitude = str(jsonResponse["Longitude"])
        zipcode = jsonResponse["PostalCode"]
        
        try:      
            if latitude and latitude != "0":
                responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                    
                responseGeocodeData = responseGeocode.json()
                zipcode = responseGeocodeData['address']['Postal']
                city = responseGeocodeData['address']['City']    
                address = responseGeocodeData['address']['LongLabel']    
            else:
                responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
                responseGeocodeData = responseGeocode.json()

                longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
                latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
                longitude = str(longitude)
                latitude = str(latitude)
                responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                    
                responseGeocodeData = responseGeocode.json()
                zipcode = responseGeocodeData['address']['Postal']
                city = responseGeocodeData['address']['City']  
                                  
        except Exception as err:
            pass 
            
    
        description = jsonResponse["Keywords"]
        
        
        images = []
        if jsonResponse['images']:
            for img in jsonResponse['images']:
                images.append(img)

        external_images_count = jsonResponse['PicturesAmount']  

        
        if "TotalParkingSpaces" in jsonResponse["Details"]:
            parking =  jsonResponse["Details"]["TotalParkingSpaces"]
            if parking and parking > 0:
                parking = True
            else:
                parking = False 
        else:
            parking = False   
        
        
        
        landlord_phone = "416-648-0090"
        landlord_email = "info@silvashometeam.com"
        landlord_name = "HomeLife/City Hill Realty Inc., Brokerage - Broker (Jonathan Silva)"


        if rent: 
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", external_link)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("square_meters", int(int(square_meters)*10.764))
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("parking", parking)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
