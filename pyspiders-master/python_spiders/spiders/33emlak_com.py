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
    name = '33emlak_com'    
    execution_type='testing'
    country='turkey'
    locale='tr' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.33emlak.com.tr/kiralik/daire",
                    "https://www.33emlak.com.tr/kiralik/bina",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.33emlak.com.tr/kiralik/villa",
                ],
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

        for item in response.xpath("//span[@class='mobilanbilgi']/../a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "33emlak_com_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_xpath("title", "//div[@class='altbaslik']/h4/strong/text()")

        price = "".join(response.xpath("//tr/td/h3/strong/text()").extract())
        if price:
            item_loader.add_value("rent_string", price)


        address = "".join(response.xpath("//td/h5/a/text()").extract())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))

        city = response.xpath("//td/h5/a[1]/text()").get()
        if city:
            item_loader.add_value("city", city.strip())  

        bathroom_count = "".join(response.xpath("//tr[td[.='Banyo Sayısı']]/td[2]/text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_xpath("external_id", "//tr[td[.='İlan No']]/td[2]/text()")
        item_loader.add_xpath("square_meters", "//tr[td[.='M² ']]/td[2]/text()")
        item_loader.add_xpath("floor", "//tr[td[.='Bulunduğu Kat']]/td[2]/text()")

        room_count =  "".join(response.xpath("//tr[td[.='Oda Sayısı']]/td[2]/text()").extract())
        if "+" in  room_count:
            item_loader.add_value("room_count", split_room(room_count, "count"))
        else:
            item_loader.add_value("room_count", room_count)


        desc = "".join(response.xpath("//div[@class='ilanaciklamalar']/p/text()").extract())
        item_loader.add_value("description", desc.strip())

        images = [response.urljoin(x)for x in response.xpath("//div[@class='clearfix']//li/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        furnished = response.xpath("//tr[td[.='Eşyalı mı?']]/td[2]/text()[.='Evet']").get()
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)

        swimming_pool = response.xpath("//span[@id='ozellikaktif']/text()[contains(.,'Yüzme Havuzu')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        else:
            item_loader.add_value("swimming_pool", False)

        lat = response.xpath("//input[@id='g_lat']/@value").get()
        if lat:
            item_loader.add_value("latitude", lat)
        
        lng = response.xpath("//input[@id='g_lng']/@value").get()
        if lng:
            item_loader.add_value("longitude", lng)

        parking = response.xpath("//span[@id='ozellikaktif']/text()[contains(.,'Otopark')]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)

        balcony = response.xpath("//span[@id='ozellikaktif']/text()[contains(.,'Balkon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)

        dishwasher = response.xpath("//span[@id='ozellikaktif']/text()[contains(.,'Beyaz Eşya')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
            item_loader.add_value("washing_machine", True)
        else:
            item_loader.add_value("dishwasher", False)
            item_loader.add_value("washing_machine", False)

        elevator = response.xpath("//span[@id='ozellikaktif']/text()[contains(.,'Asansör ')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        else:
            item_loader.add_value("elevator", False)

        item_loader.add_value("landlord_phone", "+90 (545) 933 30 33")
        item_loader.add_value("landlord_email", "info@33emlak.com.tr")
        item_loader.add_value("landlord_name", "33Emlak")

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

        
       

        
        
          

        

      
     