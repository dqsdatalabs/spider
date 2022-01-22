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
import dateparser

class MySpider(Spider):
    name = 'housing4you_eu'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.housing4you.eu/woningaanbod?status=rent&house_type=Appartement",
                    "https://www.housing4you.eu/woningaanbod?status=rent&house_type=Beneden%20+%20bovenwoning",
                    "https://www.housing4you.eu/woningaanbod?status=rent&house_type=Bovenwoning",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.housing4you.eu/woningaanbod?status=rent&house_type=1-gezinswoning",
                    "https://www.housing4you.eu/woningaanbod?status=rent&house_type=2-onder-1-kap%20woning",
                    "https://www.housing4you.eu/woningaanbod?status=rent&house_type=Hoekhuis",
                    "https://www.housing4you.eu/woningaanbod?status=rent&house_type=Landhuis",
                    "https://www.housing4you.eu/woningaanbod?status=rent&house_type=Penthouse",
                    "https://www.housing4you.eu/woningaanbod?status=rent&house_type=Tussenwoning",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.housing4you.eu/woningaanbod?status=rent&house_type=Kamer",
                ],
                "property_type" : "room"
            },
            {
                "url" : [
                    "https://www.housing4you.eu/woningaanbod?status=rent&house_type=Studio",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                
                house_type = item.split("&house_type=")[1].split("&")[0].strip()
                formdata = {
                    "action": "all_houses",
                    "api": "2fbba28ecd4596f2ee92aeea9831afde",
                    "filter": f"status=rent&house_type={house_type}",
                    "offsetRow": "0",
                    "numberRows": "10",
                    "leased_wr_last": "true",
                    "leased_last": "true",
                    "sold_wr_last": "true",
                    "sold_last": "true",
                    "path": f"/woningaanbod?status=rent&house_type={house_type}",
                    "html_lang": "nl",
                }
                yield FormRequest(
                    "https://cdn.eazlee.com/eazlee/api/query_functions.php",
                    callback=self.parse,
                    formdata=formdata,
                    meta={
                        "house_type":house_type,
                        "property_type":url.get("property_type")
                    },
                )


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 10)
        seen = False
        try:
            data = json.loads(response.body)
            for item in data:
                status = item["front_status"]
                if status and "verhuurd" in status.lower():
                    continue
                city = item['city']
                zipcode = item['zipcode']
                available_at = item['available_at']
                interior = item['interior']
                external_id = item['id']
                surface = item['surface']
                rent = item['set_price']
                bathrooms = item['bathrooms']
                bedrooms = item['bedrooms']
                image = item['photobig']
                follow_url = f"https://www.housing4you.eu/woning?{item['city']}/{item['street']}/{item['house_id']}"
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"],"city":city,"zipcode":zipcode,"available_at":available_at,"external_id":external_id,"surface":surface,"bathrooms":bathrooms,"bedrooms":bedrooms,"interior":interior,"rent":rent,"image":image})
                seen = True
        except:
            pass

        if page == 10 or seen:
            house_type = response.meta["house_type"]
            formdata = {
                    "action": "all_houses",
                    "api": "2fbba28ecd4596f2ee92aeea9831afde",
                    "filter": f"status=rent&house_type={house_type}",
                    "offsetRow": str(page),
                    "numberRows": "10",
                    "leased_wr_last": "true",
                    "leased_last": "true",
                    "sold_wr_last": "true",
                    "sold_last": "true",
                    "path": f"/woningaanbod?status=rent&house_type={house_type}",
                    "html_lang": "nl",
                }
            yield FormRequest(
                "https://cdn.eazlee.com/eazlee/api/query_functions.php",
                callback=self.parse,
                formdata=formdata,
                meta={
                    "page":page+10,
                    "house_type":house_type,
                    "property_type":response.meta.get("property_type")
                },
            )  
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Housing4you_PySpider_netherlands")
        item_loader.add_value("room_count", response.meta.get("bedrooms"))
        item_loader.add_value("city", response.meta.get("city"))
        item_loader.add_value("zipcode", response.meta.get("zipcode"))
        item_loader.add_value("external_id", response.meta.get("external_id"))
        item_loader.add_value("square_meters", response.meta.get("surface"))
        item_loader.add_value("bathroom_count", response.meta.get("bathrooms"))
        item_loader.add_value("address", "{} {}".format(response.meta.get("city"),response.meta.get("zipcode")))
        furnished = response.meta.get("interior")
        if furnished:
            item_loader.add_value("furnished",True)

        available_date=response.meta.get("available_at")
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)


        rent = response.meta.get("rent")
        if rent:
            price =  rent.replace(".","").split(",")[0].strip()
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")

        images = response.meta.get("image")
        image = []         
        if images:
            image.append(images)
            item_loader.add_value("images", image) 

        item_loader.add_value("landlord_phone", "0611733989")
        item_loader.add_value("landlord_name", "Housing 4 You")
        item_loader.add_value("landlord_email", "info@housing4you.eu")  

        yield item_loader.load_item()