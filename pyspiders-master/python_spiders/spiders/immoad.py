# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import json
from scrapy.spiders import SitemapSpider 
from ..loaders import ListingLoader
from ..helper import *
from scrapy import Request,FormRequest
from scrapy import Spider
 
class ImmoadSpider(Spider):
    name = "immoad"
    start_urls = ["https://www.immoad.be/page-data/nl/te-huur/page-data.json"] 
    allowed_domains = ["immoad.be"]
    execution_type = "testing"
    country = "belgium" 
    locale = "nl"

    def parse(self,response):
        json_data=json.loads(response.body)
        for data in json_data["result"]["pageContext"]["data"]["contentRow"][0]["data"]["propertiesList"]:
            city=data["City"].lower().replace("-","")
            desc=data["TypeDescription"].strip().lower().replace(":","").replace(",","").replace(" ","-")
            id=data["ID"]
            follow_url=f"https://www.immoad.be/page-data/nl/te-huur/{city}/{desc}/{id}/page-data.json"
            yield Request(follow_url,callback=self.populate_item,meta={"city": city, "desc": desc, "id":id})
            

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        
        json_data = json.loads(response.text)
        for item in json_data["result"]["pageContext"]["data"]["contentRow"]:
            item_loader = ListingLoader(response=response)
            item_loader.add_value(
                "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
            )
            city=response.meta.get("city")
            desc=response.meta.get("desc")
            id=response.meta.get("id")
            item_loader.add_value("external_link", f"https://www.immoad.be/nl/te-huur/{city}/{desc}/{id}/")
            item = item["property"]
            if "Appartement" in item["MainTypeName"] or "Studio" in item["MainTypeName"]:
                property_type = "apartment"
                item_loader.add_value("property_type", property_type)
            elif "Woning" in item["MainTypeName"]:
                property_type = "house"
                item_loader.add_value("property_type", property_type)
            else:
                return
            

            if item["NumberOfBedRooms"]!=0:
                item_loader.add_value("room_count", item["NumberOfBedRooms"])
            item_loader.add_value("bathroom_count", item["NumberOfBathRooms"])
            if item["Floor"]!=0:
                item_loader.add_value("floor", str(item["Floor"]))
            item_loader.add_value("city", item["City"])
            item_loader.add_value("zipcode", item["Zip"])
            item_loader.add_value("latitude", item["GoogleX"])
            item_loader.add_value("longitude", item["GoogleY"])
            item_loader.add_value("square_meters", item["SurfaceTotal"])
            item_loader.add_value("currency", "EUR")
            item_loader.add_value("description", item["DescriptionA"])
            item_loader.add_value("title", item["TypeDescription"])
            item_loader.add_value("external_id", str(item["ID"]))
            item_loader.add_value("address", " ".join([str(item["HouseNumber"]), item["Street"]]))
            item_loader.add_value("rent", item["Price"])
            item_loader.add_value("available_date", item["DateFree"].replace("00:00:00", ""))
            item_loader.add_value("images", item["LargePictures"])
            item_loader.add_value("elevator", item["HasLift"])
            item_loader.add_value(
                "parking", item["NumberOfGarages"] + item["NumberOfParkings"] + item["NumberOfExternalParkings"] > 0
            )
            item_loader.add_value("landlord_name", item["ManagerName"])
            item_loader.add_value("landlord_phone", "+32 (0)55 21 48 08")
            item_loader.add_value("landlord_email", item["ManagerEmail"])
            yield item_loader.load_item()