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
from python_spiders.helper import ItemClear


class MySpider(Spider):
    name = 'citydeal_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source="Citydeal_Co_PySpider_united_kingdom"
    start_urls = ["https://www.citydeal.co.uk/properties/?page=1&pageSize=250&orderBy=LettingsPropertyDetails.StateEnum.Value&orderDirection=ASC&propInd=L&businessCategoryId=1&searchType=grid&hideProps=1&stateValues=1"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='photo-cropped']/a"):
            status = item.xpath("./div[@class='status']/img/@alt").get()
            if status and ("agreed" in status.lower() or status.strip().lower() == "let"):
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.citydeal.co.uk/properties/?page={page}&pageSize=250&orderBy=LettingsPropertyDetails.StateEnum.Value&orderDirection=ASC&propInd=L&businessCategoryId=1&searchType=grid&hideProps=1&stateValues=1"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Citydeal_Co_PySpider_united_kingdom")
        
        item_loader.add_value("external_link", response.url.split("?")[0])
        f_text = " ".join(response.xpath("//div[@class='bedswithtype']//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[@class='description']//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//div[@class='address']/text()").get()
        if address:
            item_loader.add_value("address", address)
            
            
            zipcode = address.strip().split(" ")
            zipcode = zipcode[-2]+" "+zipcode[-1]
            if "," in zipcode:
                zipcode = zipcode.split(",")[-1].strip()
                if not any(z for z in zipcode if z.isnumeric()):
                    zipcode = ''
            elif  zipcode.split(" ")[0].isalpha():
                if zipcode.split(" ")[-1].isalpha(): zipcode = ""
                else: zipcode = zipcode.split(" ")[-1]

            if zipcode:    
                item_loader.add_value("zipcode", zipcode)
            
            city = " ".join(address.split(" ")[-2:])
            if "," in city:
                item_loader.add_value("city", city.split(",")[-1].strip())
            else:
                item_loader.add_value("city", city.strip()) 
                
        rent = "".join(response.xpath("//span[@class='displaypricequalifier']//text()").getall())
        if rent:
            if "pw" in rent.lower():
                ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='displayprice']//text()", input_type="F_XPATH", get_num=True, per_week=True, replace_list={",":""})
            elif "pcm" in rent.lower():
                ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='displayprice']//text()", input_type="F_XPATH", get_num=True, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        
        if prop_type == "studio":
            item_loader.add_value("room_count", "1")
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[@class='beds']//text()", input_type="M_XPATH", replace_list={",":""})
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[@class='bathrooms']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='reference']//text()", input_type="F_XPATH", split_list={":":1})

        desc = " ".join(response.xpath("//div[@class='description']//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
    
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='propertyimagelist']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[@id='hiddenfloorplan']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//a[contains(@href,'lng=')]/@href", input_type="M_XPATH", split_list={"lat=":1, "&":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//a[contains(@href,'lng=')]/@href", input_type="M_XPATH", split_list={"lng=":1, "&":0})
        
        features = "".join(response.xpath("//div[@class='summary']//text()").getall())
        if features:
            if "floor" in features:
                floor = features.split("floor ")[0].split("•")[-1].strip()
                if " " not in floor:
                    item_loader.add_value("floor", floor)
            
            if " furnished" in features or "Furnished" in features:
                item_loader.add_value("furnished", True)
                
            if "parking" in features.lower():
                item_loader.add_value("parking", True)
            
            if "terrace" in features.lower():
                item_loader.add_value("terrace", True)
            
            if "balcony" in features.lower():
                item_loader.add_value("balcony", True)
            
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='summary']//text()[contains(.,'Available')]", input_type="M_XPATH", split_list={"Available":1, "•":0})
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[@class='summary']//text()[contains(.,'EPC')]", input_type="M_XPATH", split_list={"EPC Rating":1, "•":0})

         
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="CITY IDEAL ESTATES", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="020 8896 0800", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="mail@cityideal.co.uk", input_type="VALUE")

        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None