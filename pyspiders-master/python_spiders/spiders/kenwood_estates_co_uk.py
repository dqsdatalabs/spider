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
    name = 'kenwood_estates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        yield Request("https://www.kenwood-estates.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='searchBtnRow']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
            seen = True
   
        if page == 2 or seen:
            yield Request(f"https://www.kenwood-estates.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=&page={page}", callback=self.parse, meta={"page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        property_type = " ".join(response.xpath("//section[@class='fullDetailWrapper']/article/text()").getall()).strip()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return

        item_loader.add_value("external_source", "Kenwood_Estates_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        title = " ".join(response.xpath("//h3//text()").getall())
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//h3//text()").get()
        if address:
            city = address.split(",")[-2].strip()
            zipcode = address.split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = "".join(response.xpath("//div[contains(@class,'fdPrice')]//div/text()").getall())
        if rent:
            rent = rent.split("£")[1].strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'fullDetailOuterBody')]//following-sibling::article/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if get_p_type_string(property_type) == "studio":
            item_loader.add_value("room_count", "1")
        else:
            room_count = response.xpath("//div[contains(@class,'fdRooms')]//span[contains(.,'bed')]/text()").get()
            if room_count:
                room_count = room_count.strip().split(" ")[0]
                if room_count > "0":
                    item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[contains(@class,'fdRooms')]//span[contains(.,'bath')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'property-photos')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//div[contains(@id,'floorplanModal')]//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        parking = response.xpath("//ul[contains(@class,'keyFeat')]//li[contains(.,'Parking') or contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//ul[contains(@class,'keyFeat')]//li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        item_loader.add_value("landlord_name", "KENWOOD ESTATE AGENTS")
        item_loader.add_value("landlord_phone", "020 7402 3141")
        item_loader.add_value("landlord_email", "property@kenwood-estates.co.uk")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None