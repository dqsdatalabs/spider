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
    name = 'dgre_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ['https://www.dethridgegroves.com.au/rent/properties-for-lease/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'listing')]//figure/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.dethridgegroves.com.au/rent/properties-for-lease/{page}/"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        description = "".join(response.xpath("//div[contains(@id,'description')]/p//text()").getall())
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        else: return
        item_loader.add_value("external_source", "Dgre_PySpider_australia")
        item_loader.add_value("external_id", response.url.split("-")[-1].split("/")[0])

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = " ".join(response.xpath("//div[contains(@class,'address-wrap')]//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address.strip())

        city = response.xpath("//div[contains(@class,'address-wrap')]//div[contains(@class,'suburb')]//text()").get()
        if city:
            item_loader.add_value("city", city.strip())

        rent = response.xpath("//span[contains(@class,'field')][contains(.,'Price')]//following-sibling::span[contains(@class,'value')]//text()").get()
        if rent and "$" in rent:
            rent = rent.replace("$","").strip().split(" ")[0]
            rent = int(rent)*4
            item_loader.add_value("rent", rent)
        elif "under" in rent.lower() or "pending" in rent.lower():
            return
        item_loader.add_value("currency", "AUD")

        desc = " ".join(response.xpath("//div[contains(@id,'property-description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//li[contains(@class,'bedrooms')]//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[contains(@class,'bathrooms')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'gallery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//span[contains(@class,'field')][contains(.,'Date Available')]//following-sibling::span[contains(@class,'value')]//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//li[contains(@class,'carspace')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = "".join(response.xpath("//div[contains(@id,'property-description')]//p//text()[contains(.,'balcon') or contains(.,'Balcon')]").getall())
        if balcony:
            item_loader.add_value("balcony", True)

        elevator = response.xpath("//div[contains(@id,'property-description')]//p//text()[contains(.,'lift') or contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
            
        dishwasher = "".join(response.xpath("//div[contains(@id,'property-description')]//p//text()[contains(.,'dishwasher') or contains(.,'Dishwasher')]").getall())
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(",")[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_email", "	info@dgre.com.au")

        landlord_name = response.xpath("//div[contains(@class,'agent')]//p[contains(@class,'name')]//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//i[contains(@class,'mobile')]//parent::p//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())

        status = response.xpath("//li[span[contains(.,'Type')]]/span[2]/text()").get()
        if not "commercial" in status.lower():
            yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("villa" in p_type_string.lower() or "bedroom" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None