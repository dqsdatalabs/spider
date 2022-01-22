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
    name = 'apartmentsflorence_it'
    external_source = "Apartmentsflorence_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it'
    start_urls = ['https://www.apartmentsflorence.it/apartment/long-term']  # LEVEL 1

    def start_requests(self):
        
        yield Request(
            url=self.start_urls[0],
            callback=self.parse,
            
        )
    
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[@class='explor_item']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = "https://www.apartmentsflorence.it/apartment?search=1&ajax=true"
            formdata = {
                "page": f"{page}",
                "range": "550;18000",
                "order": "relevance",
                "long_term": "true"
            }
            yield FormRequest(
                url=url,
                dont_filter=True, 
                callback=self.parse,
                formdata=formdata,
                meta={"page": page+1}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("substring-after(//li[contains(.,'Type')]//text(),':')").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            print(property_type)
            return
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//h4/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        address = response.xpath("//div[@id='in-book']/p[1]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())
                
        square_meters = response.xpath("//i[contains(@class,'fa-home')]/span/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//i[contains(@class,'fa-bed')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//i[contains(@class,'fa-bath')]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = response.xpath("//p[contains(.,'month')]/b/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[1].strip())
        item_loader.add_value("currency", "EUR")
        
        desc = "".join(response.xpath("//h3[contains(.,'Description')]/following-sibling::p//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))

        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat:')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng:')[1].split('\t')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
    
        floor = response.xpath("substring-after(//li[contains(.,'Floor')]//text(),':')").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        elevator = "".join(response.xpath("//li[contains(.,'Elevator')]//text()").getall())
        if elevator and "yes" in elevator.lower():
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//i[contains(@class,'check')]/following-sibling::text()[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        dishwasher = response.xpath("//i[contains(@class,'check')]/following-sibling::text()[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        washing_machine = response.xpath("//i[contains(@class,'check')]/following-sibling::text()[contains(.,'Ascensore')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        elevator = response.xpath("//i[contains(@class,'check')]/following-sibling::text()[contains(.,'Wash')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        images = [x for x in response.xpath("//div[@class='room_image']/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "APARTMENTS FLORENCE Srl")
        item_loader.add_value("landlord_phone", "+39 055 2479309")
        item_loader.add_value("landlord_email", "info@apartmentsflorence.it")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "condiviso" in p_type_string.lower():
        return "room"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartament" in p_type_string.lower() or "flat" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "casa" in p_type_string.lower() or "villetta" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    else:
        return None