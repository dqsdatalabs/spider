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
from datetime import datetime
from word2number import w2n

class MySpider(Spider):
    name = 'kpmakelaars_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    post_urls = "https://cdn.eazlee.com/eazlee/api/query_functions.php"
    
    formdata = {
        "action": "all_houses",
        "api": "8d26f881f5008508afd604a108ea5d06",
        "filter": "status=rent",
        "offsetRow": "0",
        "numberRows": "10",
        "leased_wr_last": "false",
        "leased_last": "false",
        "sold_wr_last": "false",
        "sold_last": "false",
        "path": "/woningaanbodt",
        "html_lang": "nl"
    }
    def start_requests(self):
        yield FormRequest(self.post_urls,
                    callback=self.parse,
                    formdata=self.formdata) 

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 10)
        seen = False
        data = json.loads(response.body)
        if data:
            for item in data:
                f_url = f"https://www.kpmakelaars.nl/woning?{item['city']}/{item['street'].replace(' ','-')}/{item['house_id']}"
                yield Request(f_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "item":item})
                seen = True
        
        if page==10 or seen:
            self.formdata["offsetRow"] = str(page)
            yield FormRequest(
                self.post_urls,
                formdata=self.formdata,
                dont_filter=True,
                callback=self.parse,
                meta={"page":page+10}
            )
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Kpmakelaars_PySpider_netherlands")          
        
        item = response.meta.get('item')
        property_type = item["house_type"]
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
        
        item_loader.add_value("address", f"{item['city']} {item['street']} {item['zipcode']}")
        item_loader.add_value("city", item["city"])
        item_loader.add_value("zipcode", item["zipcode"])
        
        import dateparser
        available_date = item["available_at"]
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        item_loader.add_value("square_meters", item["surface"])
        if get_p_type_string(property_type) == "room" or get_p_type_string(property_type) == "studio":
            item_loader.add_value("room_count", "1")
        else:
            item_loader.add_value("room_count", item["bedrooms"])
        
        if item["bathrooms"] == "0":
            item_loader.add_value("bathroom_count", item["bathrooms"])
            
        item_loader.add_value("external_id", item["house_id"])
        
        rent = item["set_price"]
        if rent:
            rent = rent.split(",")[0].replace(".","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency","EUR")
        
        furnished = item["interior"]
        if furnished and ("gestoffered" in furnished.lower() or "gemeubileerd" in furnished.lower()):
            item_loader.add_value("furnished", True)
            
        item_loader.add_value("landlord_name", "K&P Makelaars")
        item_loader.add_value("landlord_phone", "31 (0)50-850 77 39")
        item_loader.add_value("landlord_email", "info@kpmakelaars.nl")
        
        formdata = {
            "action": "property",
            "property_part": "description",
            "url": response.url,
            "path": "/woning",
            "html_lang": "nl",
        }
        yield FormRequest(self.post_urls, formdata=formdata, dont_filter=True, callback=self.get_description, meta={"item_loader": item_loader})
        
    def get_description(self, response):
        item_loader = response.meta.get('item_loader')
        data = json.loads(response.body)
        item_loader.add_value("description", data["description"])

        formdata = {
            "action": "property",
            "property_part": "photo",
            "photo_version": "2",
            "url": item_loader.get_collected_values("external_link")[0],
            "path": "/woning",
            "html_lang": "nl",
        }
        
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
        }
        
        yield FormRequest(self.post_urls, formdata=formdata, headers=headers, dont_filter=True, callback=self.get_image, meta={"item_loader": item_loader})
    
    def get_image(self, response):
        item_loader = response.meta.get('item_loader')
        data = json.loads(response.body)["photo"]
        for img in data:
            item_loader.add_value("images", img["huge"])

        yield item_loader.load_item()
        
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "kamer" in p_type_string.lower():
        return "room"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower() or "gezinswoning" in p_type_string.lower() or "benedenwoning" in p_type_string.lower() or "woonboot" in p_type_string.lower()):
        return "house"
    else:
        return None