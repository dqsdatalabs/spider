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
    name = 'abricasa_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source="Abricasa_PySpider_belgium"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.abricasa.be/page-data/te-huur/page-data.json",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)


    # 1. FOLLOWING
    def parse(self, response):

        data = json.loads(response.body)
        for item in data["result"]["pageContext"]["data"]["contentRow"][0]["data"]["propertiesList"]:
      
            if "Marquee" in item and item["Marquee"] == 4:
                continue

            if "MainTypeName" in item and "studio" in item["MainTypeName"].lower():
                prop_type = "studio"
            elif "MainTypeName" in item and ("appartement" in item["MainTypeName"].lower() or "flat" in item["MainTypeName"].lower() or "loft" in item["MainTypeName"].lower()):
                prop_type = "apartment"
            else:
                continue
            
            city = item["City"].lower().replace(" ", "-")
            desc = item["TypeDescription"].lower().replace(" ", "-")
            p_id = str(item["ID"])
            follow_url = f"https://www.abricasa.be/te-huur/{city}/{desc}/{p_id}/"
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":prop_type,"item":item, "p_id":p_id})
   
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status="".join(response.url)
        if "404" not in status:

            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_link", response.url)

            external_id="".join(response.url)
            if external_id:
                external_id="".join(external_id.split("/")[-2:-1])
                item_loader.add_value("external_id", external_id)
            item_loader.add_value("external_source", self.external_source)
            item = response.meta.get('item')
            
            item_loader.add_value("title", item["TypeDescription"]) 
            item_loader.add_value("zipcode", item["Zip"]) 
            item_loader.add_value("city",  item["City"]) 
            if item["SurfaceTotal"] != 0:
                item_loader.add_value("square_meters", item["SurfaceTotal"]) 
            item_loader.add_value("latitude", item["GoogleX"]) 
            item_loader.add_value("longitude", item["GoogleY"]) 
            if item["NumberOfBedRooms"] != 0:
                item_loader.add_value("room_count", item["NumberOfBedRooms"]) 
            elif "studio" in response.meta.get('property_type'):
                item_loader.add_value("room_count", "1") 
            item_loader.add_value("bathroom_count", item["NumberOfBathRooms"]) 
            item_loader.add_value("rent", item["Price"]) 
            item_loader.add_value("currency", "EUR") 
            images = [x for x in item["LargePictures"]]
            if images:
                item_loader.add_value("images", images)
            item_loader.add_value("title", item["TypeDescription"])        
            item_loader.add_value("description", item["DescriptionA"])        
            if item["NumberOfGarages"] > 0:
                item_loader.add_value("parking", True)  
            elif item["NumberOfGarages"] == 0:
                item_loader.add_value("parking", False)  
        
            item_loader.add_value("landlord_name", "Abricasa")
            item_loader.add_value("landlord_phone", "03 259 04 04")
            item_loader.add_value("landlord_email", "info@abricasa.be")

            p_id = response.meta["p_id"]
            payload="{\r\n    \"key\": \"9sSHvT2ZocBflgyPse/YRyXSZugrNzcjHdanUqepASg=\"\r\n}"
            yield Request(
                f"https://webstaticapi.omnicasa.com/api/client/property/{p_id}",
                callback=self.getApiForProperty,
                dont_filter=True,
                body=payload,
                headers = {
                    'Content-Type': 'application/json'
                },
                method="POST",
                meta={
                    "item_loader":item_loader,
                }
        )

    def getApiForProperty(self, response):

        item_loader = response.meta.get("item_loader")

        data = json.loads(response.body)["data"]["GetPropertyJsonResult"]["Value"]
 
        if "Street" in data and "HouseNumber" in data:
            address = data["Street"]+" "+data["HouseNumber"]+", "+data["Zip"]+" "+data["City"]
            item_loader.add_value("address", address) 
        if "ChargesRenter" in data:
            if data["ChargesRenter"] !=0:
                item_loader.add_value("utilities", data["ChargesRenter"]) 
        if "DepositAmount" in data:
            item_loader.add_value("deposit", data["DepositAmount"]) 
       
        yield item_loader.load_item() 
       
