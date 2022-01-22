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
    name = 'matthewsofchester_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.matthewsofchester.com/lettings/lettings-search/?student_home=false&Area=&MinPrice=&MaxPrice=&MinBeds=0&SearchType=lettings&Houseshare=no"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 30)
        seen = False
        for item in response.xpath("//div[@class='property_content']"):
            status = item.xpath("./div[@class='left_column']/div[@class='property_tagline']/text()").get()
            if status and ("agreed" in status.lower() or status.strip().lower() == "let"):
                continue
            follow_url = response.urljoin(item.xpath("./div[@class='left_column']//h3/a/@href").get())
            
            items = {}
            items["room_count"] = item.xpath("//p/strong[contains(.,'Bedroom')]/following-sibling::text()").get()
            
            yield Request(follow_url, callback=self.populate_item, meta={"items":items})
            seen = True
        
        if page == 30 or seen:
            p_url = f"https://www.matthewsofchester.com/lettings/lettings-results/?student_home=false&Area=&MinPrice=&MaxPrice=&MinBeds=0&SearchType=lettings&Houseshare=no&Offset={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+30})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Matthewsofchester_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//div[contains(@class,'description')]/p/text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return

        item_loader.add_value("external_id", response.url.split("?ID=")[-1])
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="M_XPATH")
        
        items = response.meta.get('items')
        room_count = items["room_count"]
        
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="M_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1/text()", input_type="M_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='price']/text()", input_type="M_XPATH", split_list={"pcm":0, "£":-1}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[contains(@class,'description ')]/p//text()[contains(.,'EPC')]", input_type="M_XPATH", split_list={" ":-1})

        desc = " ".join(response.xpath("//div[contains(@class,'description ')]/p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
            from datetime import datetime
            import dateparser
            date_parsed = ""
            if "available now" in desc.lower() or "available immediately" in desc.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            elif "available for" in desc.lower():
                available_date = desc.lower().split("available for")[1].replace("students","").replace("a group of","").replace("from","").strip().split(" ")
                available_d = available_date[0]+" "+available_date[1]+" "+available_date[2]
                date_parsed = dateparser.parse(available_d.replace(".",""), date_formats=["%d/%m/%Y"])
            elif "available from" in desc:
                available_date = desc.split("available from")[1].split(".")[0].replace(",","").strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            elif "available in" in desc:
                available_date = desc.split("available in")[1].split("in")[0].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            elif "available to a group" in desc:
                available_date = desc.split("available to a group")[1].split(",")[0].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            elif "available" in desc:
                available_date = desc.split("available")[1].split("on")[0].strip()
                date_p = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_p:
                    date_parsed = date_p
                else:
                    available_date = desc.split("available")[1].split("for")[0].strip()
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        from word2number import w2n
        if "bathroom" in desc:
            bathroom_count = desc.split("bathroom")[0].strip().split(" ")[-1]
            if "second" in bathroom_count:
                item_loader.add_value("bathroom_count", "2")
            
            try:
                item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
            except: pass
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1]
            not_list = ["lami","new","wood","and"]
            status = True
            for i in not_list:
                if i in floor.lower():
                    status = False
            if status:
                item_loader.add_value("floor", floor.replace("-","").upper())
     
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'slides')]//img/@data-src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//a[@name='floorplan']//@href", input_type="M_XPATH")
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="MATTHEWS CHESTER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01244 346226", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="lettings@matthewsofchester.com", input_type="VALUE")
        
        
        
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