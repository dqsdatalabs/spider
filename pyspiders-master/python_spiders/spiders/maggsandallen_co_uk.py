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
    name = 'maggsandallen_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Maggsandallen_Co_PySpider_united_kingdom"
    start_urls = ["https://www.maggsandallen.co.uk/search/?category=residential&instruction_type=Letting&bid%21=2&showstc=on&address_keyword=&minprice=&maxprice="]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[contains(.,'Full')]"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.maggsandallen.co.uk/search/{page}.html?category=residential&instruction_type=Letting&bid%21=2&showstc=on&address_keyword=&minprice=&maxprice="
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        url = response.url.split("?")[0]
        item_loader.add_value("external_link", url)
        item_loader.add_value("external_id", response.url.split("details/")[1].split("/")[0])
        f_text = " ".join(response.xpath("//div[@id='property-long-description']//p//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[@class='property-details']//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return
        
        title = response.xpath("//h1/span/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        from python_spiders.helper import ItemClear
        # ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Maggsandallen_Co_PySpider_united_kingdom", input_type="VALUE")
        item_loader.add_value("external_source",self.external_source)
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/span//text()", input_type="M_XPATH", replace_list={"\n":"", "\t":"", "\r":""})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/span//text()", input_type="M_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h2/text()", input_type="M_XPATH", get_num=True, replace_list={",":"", "pcm":"", "-":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        
        rooms = response.xpath("//span/strong/text()").getall()
        item_loader.add_value("room_count", rooms[0])
        item_loader.add_value("bathroom_count", rooms[0])
            
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'FURNISHED')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'GARAGE') or contains(.,'PARKING')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'AVAILABLE')]/text()", input_type="F_XPATH", split_list={"AVAILABLE":1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='carousel-inner']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//a[contains(.,'FLOORPLAN')]/@href", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="MAGGS & ALLEN", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0117 949 9000", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="agency@maggsandallen.co.uk", input_type="VALUE")
        
        desc = " ".join(response.xpath("//div[@id='property-long-description']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
            if "PET CONSIDERED" in desc:
                item_loader.add_value("pets_allowed", True)
            if "NO PETS" in desc:
                item_loader.add_value("pets_allowed", False)
            
            if "floor" in desc:
                floor = desc.lower().split("floor")[0].strip().split(" ")[-1]
                item_loader.add_value("floor", floor.capitalize())
        
        room_count = response.xpath("//li[contains(.,'BEDROOM')]/text()").get()
        studio = response.xpath("//li[contains(.,'STUDIO')]/text()").get()
        if not item_loader.get_collected_values("room_count") and room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        elif studio:
            item_loader.add_value("room_count", "1")
        elif desc:
            if "studio" in desc.lower():
                item_loader.add_value("room_count", "1")
            elif " room" in desc.lower():
                item_loader.add_value("room_count", "1")

        status = response.xpath("//h2/strong/text()").get()
        if not status or "let agreed" not in status.lower():
            yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "short term" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("double room" in p_type_string.lower() or "single room" in p_type_string.lower() or "ensuite" in p_type_string.lower()):
        return "room"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None