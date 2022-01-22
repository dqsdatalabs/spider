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
    name = 'nu_huren_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
        "PROXY_ON": True,

    }

    # start_urls = ["https://www.nu-huren.nl/rent-listings"]
    def start_requests(self):

        data = {
            "action": "all_houses",
            "api": "2c9c67ba49fd85da8d631740d26163a0",
            "filter": "&status=rent",
            "offsetRow": "0",
            "numberRows": "9",
            "leased_wr_last": "true",
            "leased_last": "true",
            "sold_wr_last": "true",
            "sold_last": "true",
            "path": "/woningaanbod",
            "html_lang": "nl"
        }

        yield FormRequest(
            url="https://cdn.eazlee.com/eazlee/api/query_functions.php",
            callback=self.parse,
            formdata=data,
            dont_filter=True,
           )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 9)
        seen = False
        data = json.loads(response.body)
        if data:
            for item in data:
                seen = True
                status = item["front_status"]
                if status and "Verhuurd" in status:
                    continue
                item_loader = ListingLoader(response=response)
                house_id = item["house_id"]
                city = item["city"].replace("'","")
                street = item["street"]
                zipcode = item["zipcode"]        
                external_link = f"https://www.nu-huren.nl/woning?{city}/{street}/{house_id}".replace(" ","-")
            
                p_type = item["house_type"]              
                
                if get_p_type_string(p_type): 
                    item_loader.add_value("property_type", get_p_type_string(p_type))
                else:
                    continue
                
                item_loader.add_value("external_source", "Nu_Huren_PySpider_netherlands")
                item_loader.add_value("title", street)
                item_loader.add_value("external_link", external_link)
                item_loader.add_value("external_id",house_id)
                if item["bedrooms"]:
                    if item["bedrooms"] == '0':
                        item["bedrooms"] = '1'
                    item_loader.add_value("room_count",item["bedrooms"])
                                   
                item_loader.add_value("bathroom_count",item["bathrooms"])
                item_loader.add_value("square_meters", item["surface"])
                item_loader.add_value("city",city )
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("address",  "{}, {}, {}".format(street,zipcode,city))
            
                item_loader.add_value("rent", item["set_price"])
                item_loader.add_value("currency", "EUR")
                furnished = item["interior"]
                if furnished and furnished == "Gemeubileerd":
                    item_loader.add_value("furnished", True)
        
                date_parsed = dateparser.parse(item["available_at"], date_formats=["%d %B %Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                
                item_loader.add_value("landlord_phone", "06-41393349")
                item_loader.add_value("landlord_email", "info@nu-huren.nl")
                item_loader.add_value("landlord_name", "Nu-Huren")

                data = {
                    "action": "property",
                    "property_part": "photo",
                    "photo_version": "2",
                    "url": external_link,
                    "path": "/woning",
                    "html_lang": "nl",
                }
                

                yield FormRequest(
                    url="https://cdn.eazlee.com/eazlee/api/query_functions.php",
                    callback=self.get_image,
                    formdata=data,
                    dont_filter=True,
                    meta={
                        "item_loader" : item_loader,
                        "external_link": external_link
                    })
            
            
        
        if page == 9 or seen:
            data = {
                "action": "all_houses",
                "api": "2c9c67ba49fd85da8d631740d26163a0",
                "filter": "&status=rent",
                "offsetRow": str(page+9),
                "numberRows": "9",
                "leased_wr_last": "true",
                "leased_last": "true",
                "sold_wr_last": "true",
                "sold_last": "true",
                "path": "/woningaanbod",
                "html_lang": "nl"
                }
         

            yield FormRequest(
                url="https://cdn.eazlee.com/eazlee/api/query_functions.php",
                callback=self.parse,
                formdata=data,
                dont_filter=True,
                meta={"page":page+9})
            
            
    def get_image(self, response):
        item_loader = response.meta.get("item_loader")
        data = json.loads(response.body)
        
        images = [x["middle"] for x in data["photo"]]
        if images:
            item_loader.add_value("images", images)

        # yield item_loader.load_item()
        data = {
                "action": "property",
                "property_part": "description",
                "url": response.meta.get("external_link"),
                "path": "/woning",
                "html_lang": "nl",
            }

        yield FormRequest(
            url="https://cdn.eazlee.com/eazlee/api/query_functions.php",
            callback=self.get_description,
            formdata=data,
            dont_filter=True,
            meta={
                "item_loader" : item_loader 
            }
        )            

    def get_description(self, response):
        item_loader = response.meta.get("item_loader")
        data = json.loads(response.body)
        
        item_loader.add_value("description", data["description"])

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
   
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower()  or "kamer" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "villa" in p_type_string.lower() or "Maisonnette" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None