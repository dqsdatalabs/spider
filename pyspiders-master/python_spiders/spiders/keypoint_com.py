# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
import re
 

class MySpider(Spider):
    name = 'keypoint_com'
    execution_type = 'testing'
    country = 'turkey'
    locale = 'tr' 
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.keypoint.com.tr/advanced-search/?keyword=&location%5B%5D=&status%5B%5D=kiralik&type%5B%5D=apartman-dairesi&type%5B%5D=bina&type%5B%5D=daire&type%5B%5D=residence&bedrooms=&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=&label%5B%5D=",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.keypoint.com.tr/advanced-search/?keyword=&location%5B%5D=&status%5B%5D=kiralik&type%5B%5D=ciftlik&type%5B%5D=kosk-konak&type%5B%5D=mustakil-ev&type%5B%5D=villa&type%5B%5D=yazlik&bedrooms=&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=&label%5B%5D=",
                "property_type" : "house"
            }, 
           
        ]# LEVEL 1
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    def parse(self, response):

        for item in response.xpath("//div[@class='item-body flex-grow-1']"):
            url = response.urljoin(item.xpath("./a/@href").extract_first())
            yield Request(url, callback=self.populate_item,meta={"property_type": response.meta.get("property_type")})
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Keypoint_PySpider_" + self.country + "_" + self.locale)

        item_loader.add_value("title", response.xpath("//h1/text()").get())
        item_loader.add_value("external_link", response.url)
        
        item_loader.add_value("property_type", response.meta.get("property_type"))
    
        meters = response.xpath("//ul/li[strong[ . ='Arazi alanı:']]/span/text()").extract_first()
        if meters:
            item_loader.add_value("square_meters", meters.split("m²")[0])
        elif not meters:    
            meters = response.xpath("//ul/li[strong[ . ='m2 si:']]/span/text()").extract_first()
            if meters:
                item_loader.add_value("square_meters", meters.split("m²")[0])
                
        item_loader.add_xpath("external_id", "//ul/li[strong[ . ='Emlak ID:']]/span/text()")

        desc = "".join(response.xpath("//div[@class='block-title-wrap'][h2[contains(.,'Açıklama')]]/following-sibling::div[@class='block-content-wrap']/p/text()[.!='+90533 095 0993']").extract())
        item_loader.add_value("description", desc.strip())
        if desc:
            if " furnished" in desc.lower():
                item_loader.add_value("furnished", True)


        price = response.xpath("//li[@class='item-price'][1]/text()").extract_first()
        if price:
            item_loader.add_value("rent_string", price)
        room_count = "".join(response.xpath("//ul/li[strong[contains(.,'Yatak odaları:') or contains(.,'Yatak odası:')  or contains(.,'Oda Sayısı:') ]]/span/text()").extract())
        if room_count:
            if "+" in  room_count:
                item_loader.add_value("room_count", split_room(room_count, "count"))
            else:
                item_loader.add_value("room_count", room_count)
        bathroom = "".join(response.xpath("//ul/li[strong[contains(.,'Banyo')]]/span/text()").extract())
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
        address =response.xpath("//div[@class='container']/address[@class='item-address']/text()").extract_first()    
        if address:
            item_loader.add_value("address",address)
            item_loader.add_value("city","İstanbul")
        else:
            item_loader.add_value("address","Mall Of Istanbul")
            item_loader.add_value("city","İstanbul")

                
        elevator = response.xpath("//ul[contains(@class,'list-3-cols')]/li/a/text()[contains(.,'Asansör')]").get()
        if elevator:
            item_loader.add_value("elevator", True)


        swimming_pool = response.xpath("//ul[contains(@class,'list-3-cols')]/li/a/text()[contains(.,'Yüzme Havuzu')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        parking = response.xpath("//ul[contains(@class,'list-3-cols')]/li/a/text()[contains(.,'Otopark')]").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//ul[contains(@class,'list-3-cols')]/li/a/text()[contains(.,'Mobilya')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        images=[x for x in response.xpath("//div[@class='top-gallery-section']//div/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        land_phone = "".join(response.xpath("//p[contains(.,'90')]/text()").getall())
        if land_phone:
            item_loader.add_value("landlord_phone", land_phone.strip())
        else:
            item_loader.add_value("landlord_phone", "+90 (212) 549 2267")
                
        item_loader.add_value("landlord_email", " info@keypoint.com.tr")
        item_loader.add_xpath("landlord_name", "normalize-space(//li[@class='agent-name']/text())")

        ext_id = response.xpath("//li/@data-propid").get()
        if ext_id:
            item_loader.add_value("external_id", ext_id)

        city = response.xpath("//address[@class='item-address']/text()").get()
        if city:
            if city.count(",") ==2:
                city = city.split(",")[-2].strip()
                if len(city.split(" ")) > 1:
                    item_loader.add_value("city", city.split(" ")[0])
                    item_loader.add_value("zipcode", city.split(" ")[1])
                else:
                    item_loader.add_value("zipcode", city)
            elif "," in city:
                item_loader.add_value("zipcode", city.split(",")[-2].strip())
                item_loader.add_value("city", city.split(",")[-4].strip())
        
        script_data = response.xpath("//script[@id='houzez-single-property-map-js-extra']//text()").get()
        if script_data:
            if "lng" in script_data:
                lat = script_data.split('lat":')[1].split(",")[0].replace('"', '')
                lng = script_data.split('lng":')[1].split(",")[0].replace('"', '')
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng)



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