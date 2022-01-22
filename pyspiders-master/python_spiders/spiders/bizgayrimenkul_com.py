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
    name = 'bizgayrimenkul_com'

    execution_type='testing'
    country='turkey'
    locale='tr' 
    
    def start_requests(self):
        start_urls = [
            {
                "url" : ["https://bizgayrimenkul.com.tr/tr/kiralik-daire"],
                "property_type" : "apartment"
            },
            {
                "url" : ["https://bizgayrimenkul.com.tr/tr/kiralik-villa"],
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

        for item in response.xpath("//div[@class='resim']/../@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        
        next_page = response.xpath("//a[@class='sagOk']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
            
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "bizgayrimenkul_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_xpath("external_id", "//ul/li[contains(.,'İlan No')]/span/text()")
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        price = "".join(response.xpath("//div[@class='detayBaslik']/div[@class='fiyat']/text()").extract()).replace(",",".")
        if price:
            item_loader.add_value("rent_string", price.strip())            

        title = "".join(response.xpath("//title/text()").extract())
        if title:
            item_loader.add_value("title", title.strip())
        
        address = " ".join(response.url.split('-')[4:7]).strip()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.strip().split(' ')[-1].strip())


        desc = "".join(response.xpath("//div[@class='detayaciklama']/h1/span//text()").extract())
        desc = desc.replace('\xa0', '')
        item_loader.add_value("description", desc.strip().replace("***** B\u0130Z GAYR\u0130MENKUL *****",""))
       
        item_loader.add_xpath("floor", "//ul/li[contains(.,'Daire Kat')]/span/text()")

        meters = "".join(response.xpath("//ul/li[contains(.,'Metrekare ')]/span/text()").extract())
        if meters:
            item_loader.add_value("square_meters", meters.split("m²")[0].strip())

        room_count =  "".join(response.xpath("//ul/li[contains(.,'Oda Sayısı ')]/span/text()").extract())
        if "+" in  room_count:
            item_loader.add_value("room_count", split_room(room_count, "count"))
        else:
            item_loader.add_value("room_count", room_count)

        bathroom=response.xpath("//ul/li[contains(.,'Banyo')]/span/text()").get()
        if bathroom:
            if int(bathroom.strip()) > 0:
                item_loader.add_value("bathroom_count", bathroom)
        
        images = [response.urljoin(x)for x in response.xpath("//div[@class='tn3 album']/ol/li/a/@href").extract()]
        if images:
            item_loader.add_value("images", images)


        item_loader.add_value("landlord_phone", "0(322) 232-3234")
        item_loader.add_value("landlord_email", "info@bizgayrimenkul.com.tr")
        item_loader.add_value("landlord_name", "Bizgayrimenkul")

        yield item_loader.load_item()

def split_room(room_count,get):
    count1 = room_count.split("+")[0]
    count2 = room_count.split("+")[1]
    if count2 !="": 
        count = int(count1)+int(count2)
        return str(count)
    else:
        count = int(count1.replace("+",""))
        return str(count)

        
       

        
        
          

        

      
     