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
    name = 'select_a_house_nl'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    external_source = "Select_A_House_PySpider_netherlands"
    post_urls = "https://cdn.eazlee.com/eazlee/api/query_functions.php"
    
    formdata = {
        "action": "all_houses",
        "api": "54d6755fb4708e526f3c1cf8feff51af",
        "filter": "status=rent&house_type=Appartement",
        "offsetRow": "0",
        "numberRows": "99",
        "leased_wr_last": "true",
        "leased_last": "true",
        "sold_wr_last": "true",
        "sold_last": "true",
        "path": "/woningaanbod?status=rent&house_type=Appartement",
        "html_lang": "nl"
    }
    other_filter = ["Woonhuis", "1-gezinswoning", "Kamer"]
    other_prop = ["house", "house", "room"]
    current_index = 0
    def start_requests(self):
        yield FormRequest(self.post_urls,
                    callback=self.parse,
                    formdata=self.formdata,
                    meta={'property_type': "apartment"})


    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        if data:
            for item in data:
                f_url = f"https://www.select-a-house.nl/woning?{item['city']}/{item['street'].replace(' ','-')}/{item['house_id']}"
                yield Request(f_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "item":item})
        
        if self.current_index < len(self.other_prop):
            self.formdata["filter"] = f"status=rent&house_type={self.other_filter[self.current_index]}"
            self.formdata["path"] = f"/woningaanbod?status=rent&house_type={self.other_filter[self.current_index]}"
            yield FormRequest(
                url=self.post_urls,
                callback=self.parse,
                dont_filter=True,
                formdata=self.formdata,
                meta={
                    "property_type":self.other_prop[self.current_index],
                }
            )
            self.current_index += 1
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)
        
        item = response.meta.get('item')
        
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
        
        item_loader.add_value("landlord_phone", "076 - 565 82 88")
        item_loader.add_value("landlord_name", "Select-a-House")
        item_loader.add_value("landlord_email", "info@select-a-house.nl")   
        
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