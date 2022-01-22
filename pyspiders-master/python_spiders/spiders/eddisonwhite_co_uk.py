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
from datetime import datetime
from word2number import w2n

class MySpider(Spider):
    name = 'eddisonwhite_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["http://eddisonwhite.co.uk/let/property-to-let/"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='module-content']/a"):
            status = item.xpath(".//div[contains(@class,'pstatus')]/span/text()").get()
            if status and "agreed" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"http://eddisonwhite.co.uk/let/property-to-let/page/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Eddisonwhite_Co_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        f_text = response.url
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[@id='module-description']//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return

        item_loader.add_value("external_id", response.url.split("property/")[1].split("/")[0])
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = "".join(response.xpath("//h1[@class='details_h1']/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
        
        rent = response.xpath("//h1[@class='details_h1']//span[contains(@class,'value')]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace(",",""))
            item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//div[@class='details-stats']/span//text()").get()
        if "studio" in room_count.lower():
            item_loader.add_value("room_count", "1")
        elif room_count.strip().isdigit():
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div[@class='details-stats']/span[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = " ".join(response.xpath("//div/h2[contains(.,'About')]/parent::div//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[contains(@class,'slickGalery')]//@data-img-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        available_date = response.xpath("//li[contains(.,'Available')]//text()").get()
        if available_date:
            if "now" in available_date.lower() or "immediately" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            available_date = available_date.split("Available")[1].replace("late","").replace("from","").strip()
            if available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
          
        floor = response.xpath("//li[contains(.,'Floor') or contains(.,'floor')]//text()").get()
        if floor:
            floor = floor.lower().split("floor")[0].strip().capitalize()
            item_loader.add_value("floor", floor)
        
        floor_plan_images = response.xpath("//img[@id='fpimg']/@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        energy_label = response.xpath("//li[contains(.,'Epc') or contains(.,'EPC')]//text()").get()
        if energy_label:
            energy_label = energy_label.strip().split(" ")[-1].replace("'","")
            item_loader.add_value("energy_label", energy_label)
        
        furnished = response.xpath("//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        terrace = response.xpath("//li[contains(.,'terrace') or contains(.,'Terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//li[contains(.,'balcony') or contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator = response.xpath("//li[contains(.,'Lift') or contains(.,'lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'data:')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat': '")[1].split("'")[0]
            longitude = latitude_longitude.split("lng': '")[1].split("'")[0]     
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "Capital Homes")
        item_loader.add_xpath("landlord_phone", "//div/h2[contains(.,'Contac')]/parent::div/a[contains(@href,'tel')]/text()")
        item_loader.add_xpath("landlord_email", "//div/h2[contains(.,'Contac')]/parent::div/a[contains(@href,'mailto')]/text()")
        
        
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