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
    name = 'wobeco_com'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = "Wobeco_PySpider_netherlands"
    thousand_separator = ','
    scale_separator = '.'       
    post_urls = "https://cdn.eazlee.com/eazlee/api/query_functions.php"
    
    formdata = {
        "action": "all_locations",
        "search": "status=rent&house_type=Appartement",
        "lang": "nl",
        "api": "59b4e38278eb8f7daac3bd00d4eff322",
        "path": "/woning-aanbod",
        "center_map": "true",
        "html_lang": "nl"
    }
    other_filter = ["Woonhuis"]
    other_prop = ["house"]
    current_index = 0
    def start_requests(self):
        yield FormRequest(self.post_urls,
                    callback=self.parse,
                    formdata=self.formdata,
                    meta={'property_type': "apartment"})


    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        if response.meta.get('property_type') == "house":
            for item in data:
                city = item["city"].replace("'","")
                street = item['street'].replace(" ","-")
                f_url = f"https://www.wobeco.com/woning?{city}/{street}/{item['house_id']}"                
                yield Request(f_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "item":item})
        else:
            for item in data:
                try:
                    city = data[item]['city'].replace("'","")
                    street = data[item]['street'].replace(" ","-")
                    f_url = f"https://www.wobeco.com/woning?{city}/{street}/{data[item]['house_id']}"                
                    yield Request(f_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "item":data[item]})
                except:
                    pass
        
        if self.current_index < len(self.other_prop):
            formdata = {
                "action": "all_houses",
                "api": "59b4e38278eb8f7daac3bd00d4eff322",
                "filter": "status=rent&house_type=Woonhuis",
                "offsetRow": "0",
                "numberRows": "10",
                "leased_wr_last": "false",
                "leased_last": "false",
                "sold_wr_last": "false",
                "sold_last": "false",
                "path": "/woning-aanbod?status=rent&house_type=Woonhuis",
                "html_lang": "nl",
            }
            yield FormRequest(
                url=self.post_urls,
                callback=self.parse,
                dont_filter=True,
                formdata=formdata,
                meta={
                    "property_type":self.other_prop[self.current_index],
                }
            )
            self.current_index += 1
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item = response.meta.get('item')
        item_loader.add_value("external_id", item["house_id"])
        
        item_loader.add_value("title", item["street"]) 
        item_loader.add_value("city", item["city"]) 
        item_loader.add_value("zipcode", item["zipcode"]) 
        
        item_loader.add_value("address", f"{item['city']} {item['street']} {item['number']} {item['zipcode']}") 

        item_loader.add_value("bathroom_count", item["bathrooms"]) 
        item_loader.add_value("room_count", item["bedrooms"]) 
        item_loader.add_value("square_meters", item["surface"]) 

        rent = item["set_price"].split(",-")[0].replace(".","")
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        available_date = item["available_at"]
        if available_date:  
            date_parsed = dateparser.parse(available_date.strip(),date_formats=["%d-%m-%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)     

        if "lat" in item:
            item_loader.add_xpath("latitude", item["lat"])
        
        if "lng" in item:
            item_loader.add_xpath("longitude", item["lng"])

        item_loader.add_value("landlord_name", "Wobeco Housing Agency")
        item_loader.add_value("landlord_phone", "070-3587476")
        item_loader.add_value("landlord_email", "info@wobeco.com")   
        
        status = item["front_status"]
        if status and "verhuurd" in status.lower():
            return
        
        formdata = {
            "action": "property",
            "property_part": "description",
            "url": response.url,
            "path": "/woning",
            "html_lang": "nl",
        }
        
        yield FormRequest(self.post_urls, callback=self.get_description, dont_filter=True, formdata=formdata, meta={"item_loader":item_loader, "base_url":response.url})
    
    def get_description(self, response):
        data = json.loads(response.body)
        description = data["description"].replace("<br />", "").replace("\n","")
        item_loader = response.meta.get('item_loader')
        item_loader.add_value("description", description)
        
        formdata = {
            "action": "property",
            "property_part": "photo",
            "photo_version": "2",
            "url": response.meta.get('base_url'),
            "path": "/woning",
            "html_lang": "nl",
        }
        
        yield FormRequest(self.post_urls, callback=self.get_photos, dont_filter=True, formdata=formdata, meta={"item_loader":response.meta.get('item_loader')})
    
    def get_photos(self, response):
        data = json.loads(response.body)
        images  = data["photo"]
        item_loader = response.meta.get('item_loader')
        for i in images:
            item_loader.add_value("images", i["middle"])
        
        yield item_loader.load_item()