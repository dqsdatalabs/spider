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
    name = 'fransenvastgoed_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = 'Fransenvastgoed_PySpider_netherlands'
    def start_requests(self):
        headers = {
            'Content-Type': 'application/json'
        }
        payload="[\r\n    \"Woningen\",\r\n    null,\r\n    [\r\n        {\r\n            \"beschikbaarheid\": \"asc\"\r\n        }\r\n    ],\r\n    0,\r\n    15,\r\n    null,\r\n    null\r\n]"
        p_url = "https://www.fransenvastgoed.nl/_api/wix-code-public-dispatcher/siteview/wix/data-web.jsw/find.ajax?gridAppId=28cd91b1-6ea9-4b85-a88c-3c7ebc0d35dd&instance=wixcode-pub.1b8d4897577719ddab45c32092b9fcd7ab55b387.eyJpbnN0YW5jZUlkIjoiYWZmOTM1Y2MtZWY3ZS00MDU3LWFhOTUtMzE5NGVmOGQwZGZjIiwiaHRtbFNpdGVJZCI6ImYxOGMzZTUzLTUwNjktNDRkYy05N2UzLTQ4MzJjNTUzMDM0YiIsInVpZCI6bnVsbCwicGVybWlzc2lvbnMiOm51bGwsImlzVGVtcGxhdGUiOmZhbHNlLCJzaWduRGF0ZSI6MTYxMDA5ODgwMDIzOSwiYWlkIjoiNzI4MTU1ZmItZjY5ZC00NjdmLTg3ZTctNmE0NjEwNGZmOGE3IiwiYXBwRGVmSWQiOiJDbG91ZFNpdGVFeHRlbnNpb24iLCJpc0FkbWluIjpmYWxzZSwibWV0YVNpdGVJZCI6Ijg1NzkzYjdkLTlkMTItNGI3ZS04NzRlLTVmMWJiZDA4MTgzOSIsImNhY2hlIjpudWxsLCJleHBpcmF0aW9uRGF0ZSI6bnVsbCwicHJlbWl1bUFzc2V0cyI6IkFkc0ZyZWUsSGFzRG9tYWluLFNob3dXaXhXaGlsZUxvYWRpbmciLCJ0ZW5hbnQiOm51bGwsInNpdGVPd25lcklkIjoiYjRkMzdlOTktNWRjZS00ZTA5LTg0ZTUtNGJiZTI4ZDJjODc3IiwiaW5zdGFuY2VUeXBlIjoicHViIiwic2l0ZU1lbWJlcklkIjpudWxsfQ==&viewMode=site"
        yield Request(
            p_url,
            callback=self.parse,
            body=payload,
            headers=headers,
            method="POST",
        )

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data["result"]["items"]:
            rented =  item["beschikbaarheid"]
            if "Verhuurd" in rented:
                return
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", "https://www.fransenvastgoed.nl/aanbodhuurwoningen?lang=nl")
            item_loader.add_value("external_source", self.external_source) 
            # detay page olmadigi icin ext url hepsinde ayni olacak, tum itemlar json dan alinacak - pendinge almayalim alinabilen herseyi json dan alalim.
            p_type = item["woningtype"]
            if get_p_type_string(p_type):
                item_loader.add_value("property_type", get_p_type_string(p_type))
            else:
                continue

            item_loader.add_value("title", item["title"]) 
            item_loader.add_value("address", item["title"])
            item_loader.add_value("zipcode", "3016 BE") 
            city = item["title"]
            if city:
                city = city.split(",")[-1].strip()
                if city:
                    item_loader.add_value("city", city) 
            item_loader.add_value("square_meters", item["aantalVierkanteMeters"])

            room_count = item["aantalSlaapkamers"]
            if room_count and room_count != "-":
                room_count = room_count.split("/")[0]
                item_loader.add_value("room_count", room_count) 
        
            rent = item["huurprijs"]
            if rent and rent !="In overleg":              
                price =  rent.split(",")[0].replace(",","").strip()
                item_loader.add_value("rent_string", price) 

            image = []
            images = item["fotos"]
            if images is not None:
                img = images.split(".jpg")[0]
                image.append(response.urljoin(img+".jpg"))
                item_loader.add_value("images", image) 

            available_date=response.xpath("//span[contains(@id,'ContentPlaceHolderMain_lblStatus')]/text()[.!='Let']").get()
            if available_date:
                date_parsed = dateparser.parse(
                    available_date, date_formats=["%d-%m-%Y"]
                )
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)

            furnished = item["uitvoering"]
            if furnished:             
                item_loader.add_value("furnished", True) 

            item_loader.add_value("landlord_phone", "0031-(0)657541069")
            item_loader.add_value("landlord_email", "info@fransenvastgoed.nl")
            item_loader.add_value("landlord_name", "Fransen Vastgoed")

            yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("bovenwoning" in p_type_string.lower() or "tussenwoning" in p_type_string.lower() or "hoekwoning" in p_type_string.lower()):
        return "house"
    elif p_type_string and "woning" in p_type_string.lower():
        return "house"
    else:
        return None