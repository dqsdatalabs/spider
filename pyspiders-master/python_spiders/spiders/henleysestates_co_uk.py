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
from word2number import w2n
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = 'henleysestates_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.henleysestates.co.uk/notices?c=44&p=1&available_date=undefined&filter_attribute[numeric][2][min]="]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='property_thumb']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.henleysestates.co.uk/notices.php?c=44&p={page}&&filter_attribute%5Bnumeric%5D%5B2%5D%5Bmin%5D="
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
    
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Henleysestates_Co_PySpider_united_kingdom")

        f_text = " ".join(response.xpath("//div[contains(text(),'Property Type')]/text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[@class='ch_detail_content']//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)
            else:
                f_text = response.xpath("//title/text()").get()
                if get_p_type_string(f_text):
                    prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return
                
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//h5[contains(@class,'h5_strip')]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            if "," in address:
                zipcode = address.split(",")[-1].strip()
                if zipcode.count(" ") >1:
                    item_loader.add_value("city", zipcode.split(" ")[0])
                    if not zipcode.split(" ")[-2].isalpha():
                        item_loader.add_value("zipcode", zipcode.split(" ")[-2]+zipcode.split(" ")[-1])
                    else:
                        item_loader.add_value("zipcode", zipcode.split(" ")[-1])
                elif not zipcode.split(" ")[0].isalpha():
                    item_loader.add_value("zipcode", zipcode)
                    item_loader.add_value("city", address.split(",")[-2].strip().split(" ")[-1])
                else:
                    item_loader.add_value("zipcode", zipcode.split(" ")[-1])
                    item_loader.add_value("city", zipcode.split(" ")[-2])
            else:
                item_loader.add_value("zipcode", address.split(" ")[-1])
                item_loader.add_value("city", address.split(" ")[-2])
                
        rent = response.xpath("//h5[not(contains(@class,'h5_strip'))]/text()").get()
        if rent:
            if "pw" in rent:
                price = rent.split("pw")[0].split("£")[1].strip()
                item_loader.add_value("rent", int(price)*4)
            elif "pcm" in rent:
                price = rent.split("pcm")[0].split("£")[1].strip().replace(",","")
                item_loader.add_value("rent", price)
                
            item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//div[contains(text(),'Bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        else:
            room_count = response.xpath("//div[contains(text(),'Reception')]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip().split(" ")[0])
            
        bathroom_count = response.xpath("//div[contains(text(),'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])
        
        desc = " ".join(response.xpath("//div[contains(@class,'detail_content')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1]
            if "wood" not in floor and "new" not in floor and "lami" not in floor:
                item_loader.add_value("floor", floor)
        
        images = [x for x in response.xpath("//div[@class='slider-content']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [x for x in response.xpath("//div[contains(@class,'floor_plan')]//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        external_id = response.xpath("//div[contains(@class,'available_date')]/b[contains(.,'Reference')]/parent::div/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
            
        available_date = response.xpath("//div[contains(@class,'available_date')]/b[contains(.,'Available')]/parent::div/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        latitude_longitude = response.xpath("//script[contains(.,'mymap_lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('mymap_lat=')[1].split(';')[0]
            longitude = latitude_longitude.split('mymap_lng=')[1].split(';')[0]
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        parking = response.xpath("//div[contains(text(),'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//div[contains(text(),'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        washing_machine = response.xpath("//div[contains(text(),'Washing')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        swimming_pool = response.xpath("//div[contains(text(),'Pool')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        item_loader.add_value("landlord_name", "Henleys Estates")
        item_loader.add_value("landlord_phone", "+44 (0) 208 568 4455")
        item_loader.add_value("landlord_email", "info@henleysestates.co.uk")

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "maisonette" in p_type_string.lower()  or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None