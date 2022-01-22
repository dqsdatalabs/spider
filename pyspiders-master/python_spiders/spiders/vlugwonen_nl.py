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
import re
class MySpider(Spider): 
    name = 'vlugwonen_nl'
    execution_type = 'testing'
    country = 'netherlands' 
    locale = 'nl'
    post_urls = "https://cdn.eazlee.com/eazlee/api/query_functions.php"
    external_source='Vlugwonen_PySpider_netherlands'
    
    formdata = {
        "action": "all_houses",
        "api": "ff054f45359ed0b0a31671c7852bae22",
        "filter": "status=rent",
        "offsetRow": "0",
        "numberRows": "12",
        "leased_wr_last": "false",
        "leased_last": "false",
        "sold_wr_last": "false",
        "sold_last": "false",
        "path": "/woning-aanbod",
        "html_lang": "nl"
    }
    def start_requests(self):
        yield FormRequest(self.post_urls,
                    callback=self.parse,
                    formdata=self.formdata) 

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 12)
        seen = False
        data = json.loads(response.body)
        print(data)
        if data:
            for item in data:
                f_url = f"https://www.vlugwonen.nl/woning?{item['city']}/{item['street'].replace(' ','-')}/{item['house_id']}"
                print(f_url)
                yield Request(f_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "item":item})
                seen = True
        
        if page==10 or seen:
            self.formdata["offsetRow"] = str(page)
            yield FormRequest(
                self.post_urls,
                formdata=self.formdata,
                dont_filter=True,
                callback=self.parse,
                meta={"page":page+12}
            )
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)
        item = response.meta.get('item')
        property_type = item["house_type"]
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            return
        
        item_loader.add_value("address", f"{item['city']} {item['street']} {item['zipcode']}")
        title=item_loader.get_output_value("address")
        if title:
            item_loader.add_value("title",title)

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
        
        if not item["bathrooms"] == "0":
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
        
        status = item["front_status"]
        if status and "verhuurd" not in status.lower():
            yield FormRequest(self.post_urls, formdata=formdata, dont_filter=True, callback=self.get_description, meta={"item_loader": item_loader})
        
    def get_latlong(self, response):

        item_loader = response.meta.get("item_loader")
        data = json.loads(response.body)
        if 'items' in data:
            if len(data["items"]) > 0:
                item_loader.add_value("latitude", data["lat"])
                item_loader.add_value("longitude", data["lng"])
                dontallowpark=item_loader.get_output_value("latitude")
                if dontallowpark and "parkeerplaats" in dontallowpark.lower():
                    return

        formdata = {
            "action": "property",
            "property_part": "location",
            "photo_version": "2",
            "url": "https://www.vlugwonen.nl/woning?Enschede/Doctor-Kostersstraat/H02262740080",
            "path": "/woning",
            "html_lang": "nl",
        }
        
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
        }
 
        yield FormRequest(self.post_urls, formdata=formdata, headers=headers, dont_filter=True, callback=self.get_image, meta={"item_loader": item_loader})

    def get_description(self, response):
        item_loader = response.meta.get('item_loader')
        data = json.loads(response.body)
        item_loader.add_value("description", data["description"])
        dontallowpark=item_loader.get_output_value("description")
        if dontallowpark and "parkeerplaats" in dontallowpark.lower():
            return
        deposit=data["description"]
        if deposit:
            deposit=deposit.split("Enschede")[-1].split(",")[0].replace("€","").strip()
            if deposit:
                item_loader.add_value("deposit",deposit)
 
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
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "woning" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower() or "gezinswoning" in p_type_string.lower() or "benedenwoning" in p_type_string.lower() or "woonboot" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "huis" in p_type_string.lower()):
        return "house"
    else:
        return None