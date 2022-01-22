# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser

class MySpider(Spider):
    name = 'antalyagunlukev_net'
    execution_type='testing'
    country='turkey'
    locale='tr' 
    external_source = "Antalyagunlukev_PySpider_turkey_tr"

    custom_settings = {"HTTPCACHE_ENABLED":False}
    
    def start_requests(self):
        start_urls = [
            {
                "url" : ["https://www.antalyagunlukev.net/en/for%20rent%20-monthly%20apartments%20-2-1"],
                "property_type" : "apartment"
            },
            {
                "url" : ["https://www.antalyagunlukev.net/en/for%20rent%20-villas-2-2"],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='image-slider']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
            
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        item_loader.add_value("external_source", "Antalyagunlukev_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_xpath("title","//div[@class='buy-it-now']/div/text()")

        external_id=response.xpath(
            "//div[@id='aciklama']/div/div/table//tr/td[contains(.,'İlan Numarası')]//following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        rent="".join(response.xpath(
            "//div[@class='buy-it-financial']/div/span[3]/text()").getall())
        price=rent.split(":")[1].strip()
        if price:
            if "TL" in price:
                item_loader.add_value("currency", "TRY")
                if price!="TL 0":
                    item_loader.add_value("rent", price.split("TL")[1].strip())
            elif "€" in price:
                item_loader.add_value("currency", "EUR")
                if price!="€ 0":
                    item_loader.add_value("rent", price.split("€")[1].strip())
        
            
        square_meters=response.xpath(
            "//div[@id='aciklama']/div/div/table/tbody/tr/td[contains(.,'m2')]//following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        room_count=response.xpath(
            "//div[@id='aciklama']/div/div/table//tr/td[contains(.,'Oda')]//following-sibling::td/text()").get()
        if room_count:
            if "+" in room_count:
                room_count=room_count.strip().split("+")
                item_loader.add_value("room_count", str(int(room_count[0])+int(room_count[1])))
            else:
                item_loader.add_value("room_count", room_count)
        
        bathroom=response.xpath(
            "//div[@id='aciklama']/div/div/table//tr/td[contains(.,'Banyo')]//following-sibling::td/text()"
            ).get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
        
        address="".join(response.xpath("//div[contains(@class,'ililce')]/span[2]/text()").getall())
        if address:
            city=address.split("/")[0].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city)
            
        
        desc="".join(response.xpath("//div[@class='info']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        balcony=response.xpath(
            "//ul[@class='appliances']/li/span[contains(@class,'Var')]//parent::li[contains(.,'Balkon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        images=[x for x in response.xpath("//div[@class='gallery']/div/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        latitude_longitude=response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            lat=latitude_longitude.split('LatLng(')[1].split(',')[0]
            lng=latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        
        elevator=response.xpath(
            "//ul[@class='appliances']/li/span[contains(@class,'Var')]//parent::li[contains(.,'Asansör')]/text()").get()
        if elevator:
            item_loader.add_value("elevator",True)
        
        washing_machine=response.xpath(
            "//ul[@class='appliances']/li/span[contains(@class,'Var')]//parent::li[contains(.,'Çamaşır Makinesi')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine",True)
        
        dishwasher=response.xpath(
            "//ul[@class='appliances']/li/span[contains(@class,'Var')]//parent::li[contains(.,'Bulaşık Makinesi')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher",True)
            
        parking=response.xpath(
            "//ul[@class='appliances']/li/span[contains(@class,'Var')]//parent::li[contains(.,'Otopark')]/text()").get()
        garage=response.xpath(
            "//ul[@class='appliances']/li/span[contains(@class,'Var')]//parent::li[contains(.,'Garaj')]/text()").get()
        if parking or garage:
            item_loader.add_value("parking",True)
            
        swimming_pool=response.xpath(
            "//div[@id='aciklama']/div/div/table/tbody/tr/td[contains(.,'Yüzme')]//following-sibling::td/text()").get()
        if swimming_pool=='Var':
            item_loader.add_value("swimming_pool",True)
        
        pet_allowed=response.xpath(
            "//ul[@class='appliances']/li/span[contains(@class,'Var')]//parent::li[contains(.,'Evcil Hayvan')]/text()").get()
        if pet_allowed:
            item_loader.add_value("pets_allowed",True)
        
        item_loader.add_value("landlord_name","ANTALYA GUNLUK EV")
        
        phone="".join(response.xpath("//span/h5/strong/text()").getall())
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        else:
            item_loader.add_value("landlord_phone","0530 183 58 82")
        
        yield item_loader.load_item()

        
       

        
        
          

        

      
     