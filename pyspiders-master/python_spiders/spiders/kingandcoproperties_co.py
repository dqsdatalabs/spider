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

class MySpider(Spider):
    name = 'kingandcoproperties_co'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://www.kingandcoproperties.com/search.ljson?channel=lettings&fragment=']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        data = json.loads(response.body)
        try:
            for item in data["properties"]:
                follow_url = response.urljoin(item["property_url"])
                prop_type = ""
                if get_p_type_string(item.get("property_type")):
                    prop_type = get_p_type_string(item.get("property_type"))
                    yield Request(follow_url, callback=self.populate_item, meta={'property_type': prop_type})
                seen = True
        except: pass
        if page == 2 or seen:
            url = f"https://www.kingandcoproperties.com/search.ljson?channel=lettings&fragment=page-{page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_source", "Kingandcoproperties_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-2])

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//h1//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", "Birmingham")

        rent_room = " ".join(response.xpath("//div[contains(@class,'property-description')]//h2[contains(@class,'heading__subtitle')]//text()").getall())
        if rent_room:
            if "£" in rent_room:
                rent = rent_room.split("£")[1].strip()
                if "pw" in rent.lower():
                    rent = rent.split(" ")[0]
                    rent = int(rent)*4
                else:
                    rent = rent.split(" ")[0]
                item_loader.add_value("rent", rent)
            if "bed" in rent_room:
                room_count = rent_room.split("bed")[0].strip()
                item_loader.add_value("room_count", room_count)
            if "bath" in rent_room:
                bathroom_count = rent_room.split("bath")[0].split("|")[1].strip()
                item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'property--content')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[contains(@id,'royalSlider')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking') or contains(.,'car park')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]//text()[not(contains(.,'Unfurnished'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(",")[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "KING & CO PROPERTIES")
        item_loader.add_value("landlord_phone", "0121 415 5990")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None