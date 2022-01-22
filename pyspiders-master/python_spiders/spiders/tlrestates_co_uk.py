# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
class MySpider(Spider):
    name = 'tlrestates_co_uk' 
    execution_type='testing'
    country='united_kingdom'
    locale='en'  
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://tlrestates.co.uk/"
    }
    external_source="Tlrestates_Co_PySpider_united_kingdom"
    def start_requests(self):
        start_url = "https://api.innovagent.property/propertysearch/v1/letting"
        yield Request(start_url, callback=self.parse,headers=self.headers,)
        

    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
        for item in data["data"]["Data"]:
            url_id = item["OID"]
            status = str(item["IsTenancyAdvertised"])
            if status and status=="0":
                continue
            prp_type = item["PropertyType"]
            property_type = get_p_type_string(prp_type)            
            external_link = f"https://tlrestates.co.uk/property-search#details/{url_id}"
            
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", external_link)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("title", item["FullAddress"])
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("external_id", str(url_id))
            item_loader.add_value("address", item["FullAddress"])
            item_loader.add_value("room_count", item["Bedrooms"])
            item_loader.add_value("bathroom_count", item["Bathrooms"])
            item_loader.add_value("city", item["Address3"]) 
            item_loader.add_value("zipcode", item["PostCode"])
            item_loader.add_value("deposit", item["Deposit"])
            item_loader.add_value("rent", item["PurchasePrice"])
            item_loader.add_value("currency", "GBP")
            item_loader.add_value("description", item["Description"])

            furnished = item["PropertyStatus"] 
            if furnished:
                if "unfurnished" in furnished.lower():
                    item_loader.add_value("furnished", False)
                elif "furnished" in furnished.lower():
                    item_loader.add_value("furnished", False)
            date_parsed = dateparser.parse(item["AvailabilityDate"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
            
            
            landlord_url=f"https://tlrestates.co.uk/property-search#details/{url_id}"
            yield Request(
                landlord_url, 
                callback=self.landlord_item, 
                headers=self.headers,
                meta={'item': item_loader}
            )

            emailcheck=item_loader.get_output_value("landlord_email")
            if not emailcheck:
                item_loader.add_value("landlord_email","cardiff@tlrestates.co.uk")
            phonecheck=item_loader.get_output_value("landlord_phone")
            if not phonecheck:
                item_loader.add_value("landlord_phone","02920 341077")
                
            image_url = f"https://api.innovagent.property/propertysearch/v1/letting/image/{url_id}"
            yield Request(
                image_url, 
                callback=self.image_item, 
                headers=self.headers,
                meta={'item': item_loader}
            )
       
    def image_item(self, response):
        item_loader = response.meta.get("item")
        data = json.loads(response.body)
       
        img_url = "https://propertysearch.innovagent.property/img/133/gallery/"
        images = [img_url+x["OID"]+".jpg" for x in data["data"]["Data"]["Images"]["Data"]]
        if images:
            item_loader.add_value("images", images)   
        yield item_loader.load_item()
    def landlord_item(self, response):
        item_loader = response.meta.get("item")
        landlord=response.xpath("//script[contains(.,'legalName')]/text()").get()
        if landlord:
            email=landlord.split("email")[-1].split(",")[0].replace('"',"").replace(":","")
            phone=landlord.split("telephone")[-1].split(",")[0].replace('"',"").replace(":","")
            item_loader.add_value("landlord_email",email)
            item_loader.add_value("landlord_phone",phone)
            item_loader.add_value("landlord_name","LIVING ROOM")

        
        
          
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "terraced" in p_type_string.lower()):
        return "house"
    else:
        return None