# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
class MySpider(Spider):
    name = 'harrodsestates_com'
    execution_type = 'testing' 
    country='united_kingdom'
    locale='en'
    external_source = "HarrodsEstates_PySpider_united_kingdom"

    download_timeout = 120
    custom_settings = {
        "CONCURRENT_REQUESTS" : 1,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.5,
        "AUTOTHROTTLE_MAX_DELAY": 3,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 12,
        "COOKIES_ENABLED": False,
        "LOG_LEVEL" : "DEBUG",
        "PROXY_TR_ON" : True,
    }

    def start_requests(self):
        headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/javascript",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
        }
        url = "https://www.harrodsestates.com/request/caselist/?start=0&limit=3000&salg=1&leje=1&investering=1&sort=created_on&sort_order=desc&include_raw_dates=1&solgt=0&private=1&business=1&" # LEVEL 1
        yield Request(url, headers=headers, callback=self.parse)


    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for key,value in data.items():

            if value["salesType"] == "rent":
                status = value["externalStatus"]
                if "solgt" in status:
                    continue

                url = "https://www.harrodsestates.com"+value["caseUrl"]
                property_type = ""
                propertyTypes = value["propertyTypes"]
                if "apartment" in propertyTypes.lower():
                    property_type = "apartment"
                elif "house" in propertyTypes.lower():
                    property_type = "house"
                elif "studio" in propertyTypes.lower():
                    property_type = "studio"
                else: 
                    continue

                item_loader = ListingLoader(response=response)
                item_loader.add_value("external_source", self.external_source)
                item_loader.add_value("external_link", url)

                item_loader.add_value("property_type", property_type)
                id=value["id"]
                if id:
                    item_loader.add_value("external_id",str(id))
                address=value["address"]
                if address:
                    item_loader.add_value("title",address)
                    item_loader.add_value("address",address)
                city=value["city"]
                if city:
                    item_loader.add_value("city",city)
                
                zipcode=value["zipCode"]
                if zipcode:
                    item_loader.add_value("zipcode",zipcode)
                descriptionHtml=value["descriptionHtml"]
                if descriptionHtml:
                    item_loader.add_value("description",descriptionHtml)
                rent=value["customFields"]["RentString"]
                if rent and "pcm" in rent:
                    rent = rent.split(';')[-1].split(' pcm')[0].replace(",", "")
                    item_loader.add_value("rent",rent)
                item_loader.add_value("currency","GBP")
   
                rooms=value["roomsBed"]
                if rooms:
                    item_loader.add_value("room_count",rooms)

                bathrooms=value["customFields"]["Bathrooms"]
                if rooms:
                    item_loader.add_value("bathroom_count",bathrooms)

                square_meters=value["customFields"]["SizeString"]
                if rooms:
                    item_loader.add_value("square_meters",square_meters.split('/ ')[-1].split(' ')[0])
                
                images=value["imagesDefault"]
                if images:
                    img=[]
                    for i in images:
                        img.append(i["url"])
                item_loader.add_value("images",img)
                latitude=str(value["latitude"])
                if latitude:
                    item_loader.add_value("latitude",latitude)

                longitude=str(value["longitude"])
                if longitude:
                    item_loader.add_value("longitude",longitude)
                balcon=str(value["hasBalcony"])
                if balcon=="true":
                    item_loader.add_value("balcony",True)
                elevator=str(value["hasElevator"])
                if elevator=="true":
                    item_loader.add_value("elevator",True)
                pets_allowed=str(value["petsAllowed"])
                if pets_allowed=="true":
                    item_loader.add_value("pets_allowed",True)
                available_date=value["customFields"]["AvailableFrom"]
                if available_date:
                    item_loader.add_value("available_date",available_date)
                
                item_loader.add_value("landlord_name", "Harrods Estates")
                item_loader.add_value("landlord_phone", "+44 (0) 20 7225 6506")
                item_loader.add_value("landlord_email", "enquiries@harrodsestates.com")
                yield item_loader.load_item()  
                # if url:
                #     yield Request(url, callback=self.get_details, dont_filter=True, meta={"item_loader": item_loader})
    
    # def get_details(self, response):
    #     item_loader = response.meta["item_loader"]

    #     landlord_name = response.xpath("//li[@class='name']/h4/text()").get()
    #     if landlord_name:
    #         item_loader.add_value("landlord_name", landlord_name)
    #     else:
    #         item_loader.add_value("landlord_name", "Harrods Estates")   
    #     landlord_phone = response.xpath("//li[@class='phone']/text()").get()
    #     if landlord_phone:
    #         item_loader.add_value("landlord_phone", landlord_phone)
    #     else:
    #         item_loader.add_value("landlord_phone", "+44 (0) 20 7225 6506")
    #     landlord_email = response.xpath("//li[@class='email']/a/text()").get()
    #     if landlord_email:
    #         item_loader.add_value("landlord_email", landlord_email)
    #     else:
    #         item_loader.add_value("landlord_email", "enquiries@harrodsestates.com")

    #     yield item_loader.load_item()