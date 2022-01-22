# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'cjonproperties_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["http://cjonproperties.com/property-to-rent"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 12)
        seen = False
        for item in response.xpath("//div[@class='results-grid']"):
            follow_url = response.urljoin(item.xpath("./div[contains(@class,'results-image')]/a/@href").get())
            items = {}
            items["room_count"] = item.xpath("./div[contains(@class,'results-info')]//span[contains(@class,'bedroom')]//text()").get()
            items["bathroom_count"] = item.xpath("./div[@class='results-info']//span[contains(@class,'bathroom')]/text()").get()
            items["parking"] = item.xpath("./div[@class='results-info']//span[contains(@class,'parking')]/text()").get()
            items["address"] = item.xpath("./div[@class='results-info']/h2/text()").get()
            yield Request(follow_url, callback=self.populate_item, meta={"items":items})
            seen = True
        
        if page == 12 or seen:
            p_url = f"http://cjonproperties.com/results?querytype=8&market=1&view=grid&displayperpage=12&offset={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+12})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Cjonproperties_PySpider_united_kingdom")
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("cjon-")[1].split("/")[0])
        f_text = " ".join(response.xpath("//div[@class='details-information']//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            return
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="M_XPATH")
        
        items = response.meta.get('items')
        
        room_count = items["room_count"]
        bathroom_count = items["bathroom_count"]
        address = items["address"]
        parking = items["parking"]
        
        zipcode = response.xpath("//script[contains(.,'postalCode') and contains(.,'@type\":\"House\"')]/text()").get()
        if zipcode:
            zipcode = zipcode.split('postalCode":"')[1].split('"')[0].strip()
            item_loader.add_value("zipcode", zipcode)
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2].strip())
        
        if room_count and room_count != '0':
            item_loader.add_value("room_count", room_count)
        elif prop_type == "studio":
            item_loader.add_value("room_count", "1")
            
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = response.xpath("//span[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.split("pw")[0].split("£")[-1].strip().replace(",","").replace("pcm","")
            item_loader.add_value("rent", int(float(rent))*4)
        
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")

        desc = " ".join(response.xpath("//div[contains(@class,'details-info')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
            if "short term" in desc:
                return

            if "floor" in desc:
                floor = desc.split("floor")[0].strip().split(" ")[-1].replace("(","").replace(")","")
                not_list = ["lami","tile","wood","parq", "effect"]
                status = True
                for i in not_list:
                    if i in floor.lower():
                        status = False
                if status:
                    item_loader.add_value("floor", floor.replace("-","").upper())
                    
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//td[@class='epcCurrent']/img/@src[contains(.,'energy')]", input_type="F_XPATH", split_list={"/":-1,".":0})
        
        if parking:
            if parking == 'N':
                item_loader.add_value("parking", False)
            elif parking == 'Y':
                item_loader.add_value("parking", True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'Lng')]/text()", input_type="M_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'Lng')]/text()", input_type="M_XPATH", split_list={"LatLng(":1, ",":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='sp-thumbnail']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//img[contains(@title,'Floor')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//div[@class='bullets-li']/p[contains(.,'Washing')]//text()", input_type="F_XPATH", tf_item=True)
        
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="CJON PROPERTIES", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="020 7372 4647", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@cjonproperties.com", input_type="VALUE")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None