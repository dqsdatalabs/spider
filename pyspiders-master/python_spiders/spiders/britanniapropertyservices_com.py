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
    name = 'britanniapropertyservices_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        formdata = {
            "propind": "L",
            "bedsequal": "",
            "proptown": "Birmingham",
            "RentTownList": "BIRMINGHAM",
            "SalesTownList": ":No matching properties",
            "location": "",
            "formname": "search",
            "country": "",
        }
        url = "https://www.britanniapropertyservices.com/properties.asp"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='photo']/a"):
            status = item.xpath("./div/img/@alt").get()
            if status and ("agreed" in status.lower() or status.strip().lower() == "let"):
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.britanniapropertyservices.com/properties/to-let/?Page={page}&O=Price&Dir=DESC&branch=&Country=&Location=&Town=Birmingham&Area=&MinPrice=&MaxPrice=&MinBeds=&BedsEqual=&sleeps=&propType=&Furn=&FA=&LetType=&Cat=&Avail=&searchbymap=&locations=&SS=&fromdate=&todate=&minbudget=&maxbudget="
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Britanniapropertyservices_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//div[@class='bedswithtype']//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = " ".join(response.xpath("description").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return

        address = response.xpath("//div[@class='address']/text()").get()
        if address:
            item_loader.add_value("title", address.strip())
            address = address.split(" - ")[0]
            item_loader.add_value("address", address.strip())
            zipcode = address.split(",")[-1].replace(".","").replace("(","").replace(")","").strip()
            if " " in zipcode:
                if not zipcode.split(" ")[0].isalpha() and not zipcode.split(" ")[1].isalpha():
                    if "OPP" not in zipcode and "BOUR" not in zipcode:
                        item_loader.add_value("zipcode", zipcode)
            elif not zipcode.isalpha():
                item_loader.add_value("zipcode", zipcode)
                
  
            city = ""         
            if zipcode.count(" ") == 1 and zipcode.split(" ")[0].isalpha():
                city = zipcode
            elif zipcode.count(" ") == 0 and zipcode.isalpha():
                city = zipcode
              
            if city and "Room" not in city and "Studio" not in city:
                zipcode = address.split(city)[0].replace(","," ").strip().strip().split(" ")[-1]
                if zipcode != "Mailbox" and zipcode !="Edgbaston" and zipcode!="Warward" and zipcode!="Road":
                   item_loader.add_value("zipcode", zipcode)
        
        city = response.xpath("//title/text()").get()
        if city and " in " in city:
            item_loader.add_value("city", city.split(" ")[-1])
        
        room_count = response.xpath("//div[@class='bedswithtype']/text()").get()
        if "Studio" in room_count:
            item_loader.add_value("room_count", "1")
        else:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='dateavailable']/text()", input_type="F_XPATH", split_list={":":1})
        
        term = "".join(response.xpath("//div[@class='price']//text()").getall())
        if term:
            if "week" in term:
                if "." in term:
                    ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']/text()", get_num=True, input_type="F_XPATH", per_week=True, split_list={".":0, "£":1})
                else:
                    ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']/text()", get_num=True, input_type="F_XPATH", per_week=True, split_list={"per":0, "£":1})
            elif "pcm" in term:
                ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']/text()", get_num=True, input_type="F_XPATH", split_list={".":0})
        
        desc = " ".join(response.xpath("//div[@class='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)
            
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//img/@onload", input_type="M_XPATH", split_list={'(':1, ',':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//img/@onload", input_type="M_XPATH", split_list={',':1})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@class='description']//text()[contains(.,'FURNISHED') or contains(.,'Furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@class='bedswithtype']/text()[contains(.,'Terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[@class='description']//text()[contains(.,'BALCONY') or contains(.,'balcony')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//div[@class='description']//text()[contains(.,'Dishwasher')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//div[@class='description']//text()[contains(.,'Washing')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@class='description']//text()[contains(.,'PARKING') or contains(.,'Parking') or contains(.,'parking')]", input_type="F_XPATH", tf_item=True)

        pets_allowed = response.xpath("//ul/li[contains(.,'No Pet')]/text()").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", False)
            
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='propertyimagelist']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="BRITANNIA PROPERTY SERVICES", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="44 0121 472 2200", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="enquiries@britanniapropertyservices.com", input_type="VALUE")
          
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