# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import math
from html.parser import HTMLParser


class MySpider(Spider):
    name = 'versatiegayrimenkul_com'
    start_urls = ['https://www.versatiegayrimenkul.com/ilandetay/ilanlar.php?urunSatisDurumu=1&page=1']  # LEVEL 1
    execution_type = 'testing'
    country = 'turkey'
    locale = 'tr'
  
    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get('page', 1)

        seen = False
        for item in response.xpath("//div[@class='info_property_h']/h4/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
            
        if page == 1 or seen:
            url = f"https://www.versatiegayrimenkul.com/ilandetay/ilanlar.php?urunSatisDurumu=1&page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})
 
    # 2. SCRAPING level 2
    def populate_item(self, response):
        
        item_loader = ListingLoader(response=response)
        room=response.xpath("//ul[@class='info_details']/li[contains(.,'Oda Sayısı')]/span/text()").extract_first()
        title=response.xpath("//div[@class='description']/h4/text()").extract_first()
        if title:
            item_loader.add_value("title",title)
            if "ESYALI" in title or "EŞYALI" in title:
                item_loader.add_value("furnished",True)
      
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Versatiegayrimenkul_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_xpath("external_id","//ul[@class='info_details']/li[contains(.,'Portföy Numarası:')]/span/text()")
    
        description="".join(response.xpath("//div[contains(@class,'padding_top')]//div[contains(.,'İlan Açıklaması')]/div//text()[normalize-space()]").extract())
        if description:
            item_loader.add_value("description",description)

        if "satilik" in title.lower() or "satilik" in description.lower():
            return

        city=response.xpath("//ul[@class='info_details']/li[contains(.,'Lokasyon')]/span/text()").extract_first()
        if city:
            item_loader.add_value("city", city.split("/")[0].strip())
            item_loader.add_value("address", city.replace("/ ","").strip())

        property_type=response.xpath("//ul[@class='info_details']/li[contains(.,'İlan Tipi')]/span/text()").extract_first()
        if property_type:
            if "Villa" in property_type or "Konut" in property_type or "Yalı" in property_type:
                property_type = "house"
                item_loader.add_value("property_type",property_type)
            elif "Residence" in property_type:
                property_type = "apartment"
                item_loader.add_value("property_type",property_type)
        

        item_loader.add_xpath("square_meters", "//ul[@class='info_details']/li[contains(.,'m²')]/span/text()")

        
        if room:
            add=0
            room_array=room.split("+")
            for i in room_array:
                add += int(math.floor(float(i)))
            
            item_loader.add_value("room_count",str(add) )
    
        images = [response.urljoin(x) 
                    for x in response.xpath("//div[contains(@class,'bxslider')]/div/img/@src").extract()
                    ]
        item_loader.add_value("images", images)
        item_loader.add_xpath("bathroom_count","//ul[@class='info_details']/li[contains(.,'Banyo')]/span/text()")

        item_loader.add_xpath("rent","//ul[@class='info_details']/li[contains(.,'Fiyat')]/span/text()")
        item_loader.add_value("currency", "TRY")

        parking=response.xpath("//ul[@class='general_info']/li[text()[contains(.,'Otopark') or contains(.,'Kapalı Garaj')]]").extract_first()
        if parking:
            item_loader.add_value("parking",True)
        
        balcony=response.xpath("//ul[@class='general_info']/li[text()[contains(.,'Balkon')]]").extract_first()
        if balcony:
            item_loader.add_value("balcony",True)
        
        swimming_pool=response.xpath("//ul[@class='general_info']/li[text()[contains(.,'Havuz')]]").extract_first()
        if swimming_pool:
            item_loader.add_value("swimming_pool",True)
        
        terrace=response.xpath("//ul[@class='general_info']/li[text()[contains(.,'Teras')]]").extract_first()
        if terrace:
            item_loader.add_value("terrace",True)
        
        elevator=response.xpath("//ul[@class='general_info']/li[text()[contains(.,'Asansör')]]").extract_first()
        if elevator:
            item_loader.add_value("elevator",True)

        furnished=response.xpath("//ul[@class='general_info']/li[text()[contains(.,'Mobilya')]]").extract_first()
        if furnished:
            item_loader.add_value("furnished",True)

        machine =response.xpath("//ul[@class='general_info']/li[text()[contains(.,'Beyaz Eşya')]]").extract_first()
        
        washing_machine =response.xpath("//ul[@class='general_info']/li[text()[contains(.,'Çamaşır Makinesi')]]").extract_first()
        if washing_machine:
            item_loader.add_value("washing_machine",True)
        else:
            if machine:
                item_loader.add_value("washing_machine",True)
        
        dishwasher =response.xpath("//ul[@class='general_info']/li[text()[contains(.,'Bulaşık Makinesi')]]").extract_first()
        if dishwasher:
            item_loader.add_value("dishwasher",True)
        else:
            if machine:
                item_loader.add_value("dishwasher",True)

        
        item_loader.add_value("landlord_name","Sanem Çizmeci")
        item_loader.add_value("landlord_email","0 212 351 72 75")
        item_loader.add_value("landlord_phone","info@versatiegayrimenkul.com")

        lat_lng = response.xpath("//div[@class='map_area']/iframe/@src").get()
        if lat_lng:
            lat = response.xpath("substring-before(substring-after(//div[@class='map_area']/iframe/@src,'!2d'),'!3d')").get()
            lng = response.xpath("substring-before(substring-after(//div[@class='map_area']/iframe/@src,'!3d'),'!')").get()
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",lng)
        

        if item_loader.get_collected_values("property_type"):
            yield item_loader.load_item()