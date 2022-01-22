# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json 
from urllib.parse import urljoin
import re

class MySpider(Spider):
    name = 'myspacebarcelona_com'
    execution_type='testing'
    country='spain'
    locale='es'
    
    start_urls = ["https://www.myspacebarcelona.com/WebService.asmx/List"]
    payload = {"isMonthly":"monthly","group":1,"language":"en"}
    
    def start_requests(self):
        yield Request(self.start_urls[0], body=json.dumps(self.payload), method="POST", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        with open("debug", "wb") as f:f.write(response.body)
        for i in response.xpath("//a[contains(@href,'/barcelona-apartments/')]"):
            f_url = response.urljoin(i.xpath("./@href").get())
            rent = i.xpath(".//span[contains(.,'per') and contains(.,'€')]/text()").get()
            yield Request(f_url, callback=self.populate_item, meta={"rent": rent})
             
        
    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Myspacebarcelona_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        
        property_type = ""
        if get_p_type_string(response.url):
            property_type = get_p_type_string(response.url)
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        item_loader.add_css("title","h1")
        rent= response.meta.get('rent')
        if rent:
            if "night" in rent.lower():
                rent = rent.split("€")[0].strip()
                rent = int(rent)*30
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        studio = "".join(response.xpath("//li[@id='liHabitacions']/span/text()").extract())
        if "Studio" in studio:
            item_loader.add_value("property_type", "studio")
            item_loader.add_value("room_count", "1")
        else:
            item_loader.add_value("property_type", property_type)

        square_meters="".join(response.xpath("//ul[contains(@class,'icones')]/li[contains(.,'m²')]//text()").getall())
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m²')[0].strip())
        
        room_count = response.xpath("//li[@id='liHabitacions']/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        item_loader.add_value("address", "Barcelona")
        item_loader.add_value("city", "Barcelona")
        item_loader.add_xpath("bathroom_count", "normalize-space(//li[@id='li1']/span/text())")
             
        external_id="".join(response.xpath("//div[@class='llicencia']/div[contains(.,'Ref')]//text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.split('Ref.')[1].strip())

        desc="".join(response.xpath("//div[@id='article']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        lati=response.xpath("//script[contains(.,'let long')]/text()").get()
        if lati:
            item_loader.add_value("latitude",lati.split("lati")[-1].split(";")[0].replace("=","").strip())
        longi=response.xpath("//script[contains(.,'let long')]/text()").get()
        if longi:
            item_loader.add_value("longitude",longi.split("long")[-1].split(";")[0].replace("=","").strip())
            
        images=[x for x in response.xpath("//div[@class='swiper-wrapper']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name","MY SPACE BARCELONA")
        item_loader.add_value("landlord_phone","34 934 173 266")
        item_loader.add_value("landlord_email","info@myspacebarcelona.com")
            
        balcony=response.xpath("//ul[contains(@class,'icones')]/li[contains(.,'Balcony')]//text()").getall()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator=response.xpath("//ul[@class='list_ok']/li[contains(.,'Elevator')]/text()").getall()
        if elevator:
            item_loader.add_value("elevator",True)
        
        terrace=response.xpath("//ul[@class='list_ok']/li[contains(.,'terrace')]/text()").getall()
        if terrace:
            item_loader.add_value("terrace",True)
        
        washing_machine=response.xpath("//ul[@class='list_ok']/li[contains(.,'Washing')]/text()").getall()
        if washing_machine:
            item_loader.add_value("washing_machine",True)
        
        dishwasher=response.xpath("//ul[@class='list_ok']/li[contains(.,'Dishwasher')]/text()").getall()
        if dishwasher:
            item_loader.add_value("dishwasher",True)
            
        parking=response.xpath("//ul[@class='list_ok']/li[contains(.,'Parking')]/text()").getall()
        if parking:
            item_loader.add_value("parking",True)

        swimming_pool=response.xpath("//ul[@class='list_ok']/li[contains(.,'swimming pool')]/text()").getall()
        if swimming_pool:
            item_loader.add_value("swimming_pool",True)

        furnished =response.xpath("//ul[@class='list_ok']/li[contains(.,'Furnished')]/text()").getall()
        if furnished:
            item_loader.add_value("furnished",True)
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None