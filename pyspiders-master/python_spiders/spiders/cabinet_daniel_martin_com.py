# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
from datetime import datetime
from datetime import date
import dateparser

class MySpider(Spider):
    name = 'cabinet_daniel_martin_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.bienici.com/realEstateAds-userads.json?filters=%7B%22size%22%3A24%2C%22from%22%3A0%2C%22showAllModels%22%3Afalse%2C%22filterType%22%3A%5B%22buy%22%2C%22rent%22%5D%2C%22propertyType%22%3A%5B%22flat%22%2C%22loft%22%5D%2C%22page%22%3A1%2C%22sortBy%22%3A%22relevance%22%2C%22sortOrder%22%3A%22desc%22%2C%22onTheMarket%22%3A%5Btrue%5D%2C%22limit%22%3A%22wphqGvb%7DC%3FmtzArttAzI%3Fv~yA%22%2C%22author%22%3A%22ubiflow-easybusiness-ag330739%22%2C%22newProperty%22%3Afalse%2C%22blurInfoType%22%3A%5B%22disk%22%2C%22exact%22%5D%7D&extensionType=extendedIfNoResult&access_token=FVSVt9LKC9cMOnc3wXm5SH1RIuZopvPDKO0p966uR0E%3D%3A60485dea0b3d0200b66e8d77&id=60485dea0b3d0200b66e8d77",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.bienici.com/realEstateAds-userads.json?filters=%7B%22size%22%3A24%2C%22from%22%3A0%2C%22showAllModels%22%3Afalse%2C%22filterType%22%3A%5B%22rent%22%5D%2C%22propertyType%22%3A%5B%22house%22%5D%2C%22page%22%3A1%2C%22sortBy%22%3A%22relevance%22%2C%22sortOrder%22%3A%22desc%22%2C%22onTheMarket%22%3A%5Btrue%5D%2C%22limit%22%3A%22_yqpGldsB%3FmlJduIB%3FdlJ%22%2C%22author%22%3A%22ubiflow-easybusiness-ag330739%22%2C%22newProperty%22%3Afalse%2C%22blurInfoType%22%3A%5B%22disk%22%2C%22exact%22%5D%7D&extensionType=extendedIfNoResult&access_token=FVSVt9LKC9cMOnc3wXm5SH1RIuZopvPDKO0p966uR0E%3D%3A60485dea0b3d0200b66e8d77&id=60485dea0b3d0200b66e8d77",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
        for item in data["realEstateAds"]:
            follow_url = f"https://www.bienici.com/annonce/location/{item['id']}"
            item_loader = ListingLoader(response=response)

            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_link", follow_url.split("?")[0])

            item_loader.add_value("external_source", "Cabinet_Daniel_Martin_PySpider_france")

            if "reference" in item.keys(): item_loader.add_value("external_id", item["reference"].strip())
            address = ""
            if "city" in item.keys():
                address += item["city"].strip() + ' '
                item_loader.add_value("city", item["city"].strip())      
            if "postalCode" in item.keys():
                address += item["postalCode"].strip()
                item_loader.add_value("zipcode", item["postalCode"].strip())   
            if address: item_loader.add_value("address", address.strip())
            if "title" in item.keys(): item_loader.add_value("title", item["title"].strip())
            if "description" in item.keys(): item_loader.add_value("description", item["description"].strip())
            if "surfaceArea" in item.keys(): item_loader.add_value("square_meters", str(int(item["surfaceArea"])))
            if "bathroomsQuantity" in item.keys(): item_loader.add_value("bathroom_count", str(item["bathroomsQuantity"]))
            if "bedroomsQuantity" in item.keys(): item_loader.add_value("room_count", str(item["bedroomsQuantity"]))
            elif "roomsQuantity" in item.keys(): item_loader.add_value("room_count", str(item["roomsQuantity"]))          
            if "price" in item.keys():
                item_loader.add_value("rent", str(int(float(item["price"]))))
                item_loader.add_value("currency", 'EUR')              
            if "availableDate" in item.keys():
                date_parsed = dateparser.parse(item["availableDate"].lower().split('t')[0].strip(), date_formats=["%d/%m/%Y"], languages=['fr'])
                today = datetime.combine(date.today(), datetime.min.time())
                if date_parsed:
                    result = today > date_parsed
                    if result == True: date_parsed = date_parsed.replace(year = today.year + 1)
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            if "safetyDeposit" in item.keys(): item_loader.add_value("deposit", str(int(float(item["safetyDeposit"]))))
            if "photos" in item.keys():
                item_loader.add_value("images", [response.urljoin(x["url"]) for x in item["photos"]])
                item_loader.add_value("external_images_count", len(item["photos"]))
            if "blurInfo" in item.keys():
                if "position" in item["blurInfo"].keys():
                    if 'lat' in item["blurInfo"]["position"].keys(): item_loader.add_value("latitude", str(item["blurInfo"]["position"]["lat"]))
                    if 'lon' in item["blurInfo"]["position"].keys(): item_loader.add_value("longitude", str(item["blurInfo"]["position"]["lon"]))
                    elif 'lng' in item["blurInfo"]["position"].keys(): item_loader.add_value("longitude", str(item["blurInfo"]["position"]["lng"]))
            if "energyClassification" in item.keys(): item_loader.add_value("energy_label", item["energyClassification"].strip())
            if "floor" in item.keys(): item_loader.add_value("floor", str(item["floor"]))
            if "charges" in item.keys(): item_loader.add_value("utilities", str(item["charges"]))          
            if "isFurnished" in item.keys(): item_loader.add_value("furnished", item["isFurnished"])
            if "hasElevator" in item.keys(): item_loader.add_value("elevator", item["hasElevator"])
            if "balconyQuantity" in item.keys():
                if item["balconyQuantity"] > 0: item_loader.add_value("balcony", True)
                elif item["balconyQuantity"] == 0: item_loader.add_value("balcony", False)
            if "parkingPlacesQuantity" in item.keys():
                if item["parkingPlacesQuantity"] > 0: item_loader.add_value("parking", True)
                elif item["parkingPlacesQuantity"] == 0: item_loader.add_value("parking", False)
            item_loader.add_value("landlord_name", "Cabinet Daniel Martin")
            item_loader.add_value("landlord_phone", "05 56 85 92 13")

            yield item_loader.load_item()
    
      
            