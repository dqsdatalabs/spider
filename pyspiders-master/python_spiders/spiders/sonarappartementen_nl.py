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
    name = 'sonarappartementen_nl'
    execution_type = "testing"
    country = "netherlands"
    locale = "nl"

    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    def start_requests(self):
        
        formdata = {
            "action": "all_houses",
            "api": "613c3a5115c4a6a7231821794accf3ea",
            "filter": "status=rent&house_type=Apartment",
            "offsetRow": "0",
            "numberRows": "10000",
            "leased_wr_last": "false",
            "leased_last": "false",
            "sold_wr_last": "false",
            "sold_last": "false",
            "path": "/en-gb/woningaanbod?status=rent&house_type=Apartment",
            "html_lang": "en",
        }
        yield FormRequest(
            "https://cdn.eazlee.com/eazlee/api/query_functions.php",
            callback=self.parse,
            formdata=formdata,
            headers=self.headers,
            meta={
                "property_type":"apartment",
            },
        )

        formdata = {
            "action": "all_houses",
            "api": "613c3a5115c4a6a7231821794accf3ea",
            "filter": "status=rent&house_type=House",
            "offsetRow": "0",
            "numberRows": "10000",
            "leased_wr_last": "false",
            "leased_last": "false",
            "sold_wr_last": "false",
            "sold_last": "false",
            "path": "/en-gb/woningaanbod?status=rent&house_type=House",
            "html_lang": "en",
        }
        yield FormRequest(
            "https://cdn.eazlee.com/eazlee/api/query_functions.php",
            callback=self.parse,
            formdata=formdata,
            headers=self.headers,
            meta={
                "property_type":"studio",
            },
        )


    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data:
            status = item["front_status"]
            if status and "verhuurd" in status.lower():
                continue
            follow_url = f"https://www.sonarappartementen.nl/en-gb/woning?{item['city']}/{item['street']}/{item['house_id']}".replace(" ", "-")
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1].strip())

        formdata = {
            "action": "property",
            "api": "613c3a5115c4a6a7231821794accf3ea",
            "url": response.url,
            "path": "/woning",
        }
        yield FormRequest(
            "https://cdn.eazlee.com/eazlee/api/query_functions.php",
            callback=self.get_property_info,
            formdata=formdata,
            headers=self.headers,
            dont_filter=True,
            meta={"item":item_loader},)

    def get_property_info(self, response):
        item_loader = response.meta["item"]
        item_loader.add_value("external_source", "Sonarappartementen_PySpider_netherlands")
        item_loader.add_xpath("title", "//title/text()")
        

        json_data = json.loads(response.body)
        print("-------------",json_data)
        bathroom_count = json_data[0]["bathrooms"]
        if bathroom_count and bathroom_count !="0":
            item_loader.add_value("bathroom_count", bathroom_count)
        room_count = json_data[0]["bedrooms"]
        if room_count and room_count !="0":
            item_loader.add_value("room_count", room_count)
        else:
            if "Kamers" in json_data[2]:
                room_count = json_data[2]["Kamers"]
                if room_count and room_count !="0":
                    item_loader.add_value("room_count", room_count)
        if "surface" in json_data[0]:
            square_meters = json_data[0]["surface"]
            item_loader.add_value("square_meters",square_meters)
        zipcode = ""
        if "zipcode" in json_data[0]:
            zipcode = json_data[0]["zipcode"]
            item_loader.add_value("zipcode", zipcode)
        city = ""
        if "city" in json_data[0]:
            city = json_data[0]["city"]
            item_loader.add_value("city", city)
        street = ""
        if "street" in json_data[0]:
            street = json_data[0]["street"]
            city = street +", "+zipcode + ", "+city
        item_loader.add_value("address",city )    
        
        rent = json_data[0]["set_price"]
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        
        if "available_at" in json_data[0]:  
            available = json_data[0]["available_at"]
            date_parsed = dateparser.parse(available, date_formats=["%d-%m-%Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        desc = json_data[3]["description"]
        if desc:
            item_loader.add_value("description", desc)
        if "Balkon:" in json_data[4]:  
            balcony = json_data[4]["Balkon:"]
            if "nee" in balcony.lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)
        if "Energie label" in json_data[2]:  
            energy_label = json_data[2]["Energie label"]
            item_loader.add_value("energy_label", energy_label)
        
        if "Dakterras:" in json_data[4]:  
            terrace = json_data[4]["Dakterras:"]
            if "nee" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        if "Huisdieren:" in json_data[4]:  
            pets_allowed = json_data[4]["Huisdieren:"]
            if "nee" in pets_allowed.lower():
                item_loader.add_value("pets_allowed", False)
            else:
                item_loader.add_value("pets_allowed", True)
        
        if "Interieur:" in json_data[4]:  
            furnished =json_data[4]["Interieur:"]
            if "gemeubileerd" in furnished.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        if "Borg:" in json_data[4]:  
            deposit =json_data[4]["Borg:"].replace(".","").replace(",",".").replace("€","")
            item_loader.add_value("deposit", int(float(deposit.strip())))
        if "lat" in json_data[5]:  
            lat =json_data[5]["lat"]
            item_loader.add_value("latitude", str(lat))

        if "lng" in json_data[5]:  
            lng = json_data[5]["lng"]
            item_loader.add_value("longitude", str(lng))
        
        images = [x["middle"] for x in json_data[1]]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Sonar Appartementen")
        item_loader.add_value("landlord_phone", "+31(0) 70 - 350 50 80")
        item_loader.add_value("landlord_email", "info@sonarappartementen.nl")
        
        yield item_loader.load_item()

        
        
