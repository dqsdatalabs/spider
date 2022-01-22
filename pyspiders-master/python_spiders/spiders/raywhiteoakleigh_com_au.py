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
import dateparser
class MySpider(Spider):
    name = 'raywhiteoakleigh_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    custom_settings = {         
        # "PROXY_ON": True,        
        "PASSWORD": "1yocxe3k3sr3",        
        # "HTTPCACHE_ENABLED": False,        
        "LOG_LEVEL": "DEBUG",        
        "FEED_EXPORT_ENCODING": "utf-8",        
        "CONCURRENT_REQUESTS": 3,        
        "COOKIES_ENABLED": False,        
        "AUTOTHROTTLE_ENABLED": True,        
        "AUTOTHROTTLE_START_DELAY": .1,        
        "AUTOTHROTTLE_MAX_DELAY": .3,        
        "RETRY_TIMES": 3,        
        "DOWNLOAD_DELAY": 3,    
    }
    handle_httpstatus_list = [429]
    url = "https://raywhiteapi.ep.dynamics.net/v1/listings?apiKey=FB889BB8-4AC9-40C2-829A-DD42D51626DE"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        }
    def start_requests(self):
        infos = [
            {
                "payload" : {"size":50,"statusCode":"CUR","typeCode":["COM","HOL","REN"],"categoryCode":["APT","UNT"],"subTypeCode":{"not":["SAL"]},"sort":["updatedAt desc","id desc","_score desc"],"organisationId":[319,1384,2528],"from":0},
                
                "property_type" : "apartment",
            },
            {
                "payload" :{"size":50,"statusCode":"CUR","typeCode":["COM","HOL","REN"],"categoryCode":["HSE","THS"],"subTypeCode":{"not":["SAL"]},"sort":["updatedAt desc","id desc","_score desc"],"organisationId":[319,1384,2528],"from":0},
                "property_type" : "house",
            },
            {
                "payload" :{"size":50,"statusCode":"CUR","typeCode":["COM","HOL","REN"],"categoryCode":["STD"],"subTypeCode":{"not":["SAL"]},"sort":["updatedAt desc","id desc","_score desc"],"organisationId":[319,1384,2528],"from":0},
                "property_type" : "studio",

            },
            
        ]
        for item in infos:
            yield Request(self.url,
                        method="POST",
                        headers=self.headers,
                        body=json.dumps(item["payload"]),
                        dont_filter=True,
                        callback=self.parse,                   
                        meta={'property_type': item["property_type"], 'payload': item["payload"]})


    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)     
        total_page = data["hits"]
        for item in data["data"]:
            p_id = item["value"]["id"] 
            stateCode = item["value"]["address"]["stateCode"]
            suburb = item["value"]["address"]["suburb"]
            suburbId = item["value"]["address"]["suburbId"]
            p_type = item["value"]["categories"][0]["category"]
            f_url = "https://raywhiteoakleigh.com.au/properties/residential-for-rent/"+str(stateCode)+"/"+suburb+"-"+str(suburbId)+"/"+p_type+"/"+str(p_id)

            yield Request(f_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"],"item":item})
        payload_page = response.meta["payload"]["from"]
        payload_page = int(payload_page) +50
        if payload_page < total_page:
            payload = response.meta["payload"]
            payload["from"] = payload_page
            yield Request(self.url,
                        method="POST",
                        headers=self.headers,
                        body=json.dumps(payload),
                        dont_filter=True,
                        callback=self.parse,                   
                        meta={'property_type': response.meta["property_type"], 'payload': response.meta["payload"]})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item = response.meta.get('item')
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Raywhiteoakleigh_Com_PySpider_australia")
        agent = item["value"]["agents"][0]
        if agent:
            item_loader.add_value("landlord_name", agent["fullName"])
            item_loader.add_value("landlord_email", agent["email"])
            item_loader.add_value("landlord_phone", agent["mobilePhone"])
        
        item_loader.add_value("external_id", str(item["value"]["id"]))
        item_loader.add_value("city", item["value"]["address"]["suburb"])
        if "location" in item["value"]["address"]:
            item_loader.add_value("longitude", str(item["value"]["address"]["location"]["lon"]))
            item_loader.add_value("latitude", str(item["value"]["address"]["location"]["lat"]))
         
        if "images" in item["value"]:
            images = [x["url"] for x in item["value"]["images"]]
            if images:
                item_loader.add_value("images", images)

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
    
        room_count = "".join(response.xpath("//div[contains(@class,'pdp_header')]//li[contains(@class,'bed')]//text()").getall())
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = "".join(response.xpath("//div[contains(@class,'pdp_header')]//li[contains(@class,'bath')]//text()").getall())
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)

        address = " ".join(response.xpath("//h1//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split(",")[-1].strip())

        desc = " ".join(response.xpath("//div[contains(@class,'pdp_description_content')]/p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        price = response.xpath("//span[contains(@class,'pdp_price')]//text()").get()
        if price:
            rent = price.split("Weekly")[0].split("per week")[0].split("pw")[0].split("PW")[0].replace("$","").strip().replace(",","")
            deposit = price.split("/")[1].split("$")[1].split("Bond")[0].strip().replace(",","")
            item_loader.add_value("rent", int(float(rent))*4)
            item_loader.add_value("deposit",deposit)
        item_loader.add_value("currency", "AUD")

        available = "".join(response.xpath("//div[contains(@class,'event_heading')][contains(.,'Available')]//parent::div//div[contains(@class,'event_date_wrap')]//span//text()").getall())
        if available:
            day = response.xpath("//div[contains(@class,'event_heading')][contains(.,'Available')]//parent::div//span[contains(@class,'event_date')]//text()").get()
            month = "".join(response.xpath("//div[contains(@class,'event_heading')][contains(.,'Available')]//parent::div//span[contains(@class,'event_month')]//text()").getall())
            available_date = day+" "+month
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)     

        balcony = response.xpath("//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony",True)
        terrace = response.xpath("//li[contains(.,'Terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        parking = "".join(response.xpath("//div[contains(@class,'pdp_header')]//li[contains(@class,'car')]//text()").getall())
        if parking:
            item_loader.add_value("parking",True)
               
        yield item_loader.load_item()