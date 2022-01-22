# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime
import dateparser 
import re

class MySpider(Spider):
    name = 'cityhomes_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://cityhomes.nl/index.php/tools/blocks/houses_list/ajax_houses_list?blockID=325&city=&rentPrice=0&rentPriceSliderVal=897&nrBedrooms=0&rentInterior=&rentKind=&locale=nl&_=1610004107836"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='offer-padding']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Cityhomes_PySpider_netherlands")
        status = response.xpath("//div[contains(@class,'sidebar-block leased')]//div[@class='offer-leased']/span/text()").get()
        if status and "verhuurd" in status.lower():
            return

        item_loader.add_value("external_link", response.url)
        externalid=response.url
        if externalid:
            externalid=externalid.split("/")[-1]
            item_loader.add_value("external_id",externalid)


        f_text = "".join(response.xpath("//div[@class='house-description']/p//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        
        address = " ".join(response.xpath("//h1[@class='offer-city']//text()").getall())
        if address:
            item_loader.add_value("title", address.strip())
            item_loader.add_value("address", address.strip())
        
        city = response.xpath("//h1[@class='offer-city']/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        
        rent = response.xpath("//li/i[contains(@class,'round')]/parent::li/text()").get()
        if rent:
            price = rent.split("€")[1].strip()
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//li/i[contains(@class,'drawing')]/parent::li/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())

        room_count = response.xpath("//li/i[contains(@class,'size-bed')]/parent::li/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        
        desc = " ".join(response.xpath("//div[@class='house-description']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "badkamer" in desc:
            bathroom_count = desc.split("badkamer")[0].strip().split(" ")[-1]
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
            else:
                bathroom_count = desc.split("badkamer")[1].strip().split(" ")[-1]
                if "tweede" in bathroom_count:
                    item_loader.add_value("bathroom_count", "2")
                elif bathroom_count.replace("e","").isdigit():
                    item_loader.add_value("bathroom_count", bathroom_count.replace("e",""))
        
        if "verdieping" in desc:
            floor = desc.split("verdieping")[0].strip().split(" ")[-1]
            if "tweede" in floor:
                item_loader.add_value("floor", floor)
            elif floor.replace("de","").replace("e","").isdigit():
                item_loader.add_value("floor", floor.replace("de","").replace("e",""))
        
        images = [x for x in response.xpath("//div[@id='lightgallery']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        available_date = response.xpath("//li/i[contains(@class,'calendar')]/parent::li/span/text()").get()
        if available_date:
            if "DIRECT" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        furnished = response.xpath("//li/i[contains(@class,'sofa')]/parent::li/span/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        latitude = response.xpath("//div[@id='latitude']/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//div[@id='longtitud']/text()").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        pets_allowed = response.xpath("//li[contains(@class,'Pet')]/text()").get()
        if pets_allowed:
            if "Huisdieren toegestaan" in pets_allowed:
                item_loader.add_value("pets_allowed", True)
            elif "Geen huisdieren" in pets_allowed:
                item_loader.add_value("pets_allowed", False)
            
        terrace = response.xpath("//li[contains(.,'terras') or contains(.,'Terras')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        balcony = response.xpath("//li[contains(.,'balkon') or contains(.,'Balkon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        elevator = response.xpath("//li[contains(.,'Lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        parking = response.xpath("//li[contains(.,'Garage') or contains(.,'Parkeren')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        item_loader.add_value("landlord_name", "CITYHOMES")
        item_loader.add_value("landlord_phone", "0206250071 ")
        item_loader.add_value("landlord_email", "info@cityhomes.nl")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None