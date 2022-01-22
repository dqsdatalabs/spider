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
from  geopy.geocoders import Nominatim
import re

class MySpider(Spider):
    name = 'mapartments_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Mapartments_PySpider_united_kingdom_en'
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://mapartments.co.uk/apartments/",
                "property_type" : "apartment"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'flex flex-wrap w-full')]/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        next_page = response.xpath("//a[contains(@x-ref,'scrollNext')]/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type" : response.meta.get("property_type")})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)       
        item_loader.add_value("external_link", response.url)

        prop_check = response.xpath("//h1/text()").get()
        if prop_check and "studio" in prop_check.lower():
            property_type = "studio" 
            item_loader.add_value("property_type", property_type)
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
    
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        rent="".join(response.xpath("//div[@class='text-sm font-light text-white']/text()[contains(.,'£')]").getall())
        price = ""
        if rent:
            price=rent.split("£")[1].replace("pw", "").strip()
            item_loader.add_value("rent", str(int(price)*4))
        item_loader.add_value("currency", "GBP")   
        
        if prop_check and "studio" in prop_check.lower():
            item_loader.add_value("room_count", "1")
        else:
            room_count=response.xpath("(//div[@class='text-sm font-light text-white'])[1]").get()
            if room_count:
                room_count = re.findall("\d", room_count)
                item_loader.add_value("room_count", room_count)
        
        item_loader.add_value("city", "Manchester")
        item_loader.add_value("address", "Manchester")
        
        desc="".join(response.xpath("//div[contains(@class,'w-full -mb-6')]/p/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.strip())
        if ("furnished" in desc.lower()) and ("unfurnished" not in desc.lower()):
            item_loader.add_value("furnished", True)
            
        images=[x for x in response.xpath("//picture[@class='absolute top-0 left-0 w-full h-full ']/source/@data-srcset").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        utilities = response.xpath("//div[contains(@class,'head')]//text()[contains(.,'utilities')]").get()
        if utilities:
            utilities = utilities.split("(£")[1].strip().split(" ")[0]
            utilities = str(int(utilities)-int(price))
            item_loader.add_value("utilities", utilities)
        
        deposit=response.xpath("//div[contains(@class,'head')]/span[contains(.,'Deposit')]/parent::div/div/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("£",""))
        
        item_loader.add_value("landlord_name", "MANCHESTER APARTMENTS")
        item_loader.add_value("landlord_phone", "0161 228 6633")
        item_loader.add_value("landlord_email", "hello@mapartments.co.uk")
            
        
        rent_status=response.xpath("//div[contains(@class,'head')][contains(.,'currently unavailable')]/text()").getall()
        if not rent_status:
            yield item_loader.load_item()

