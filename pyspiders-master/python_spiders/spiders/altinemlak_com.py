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
import re
import unicodedata

class MySpider(Spider):
    name = 'altinemlak_com'
    execution_type='testing'
    country='turkey'
    locale='tr'   

    def start_requests(self):
        start_urls = [
            {"url": "https://altinemlak.com.tr/portfoyler?AdTypeId=2&processTypeName=2&CityId=&Keyword="},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})



    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        for item in response.xpath("//div[contains(@class,'property-listing')]"):
            status = item.xpath(".//span[@class='property-type']//text()").get()
            if "Kiralık" in status: 
                follow_url = item.xpath(".//@href").get()
                yield Request(response.urljoin(follow_url), callback=self.populate_item)
        
        
        next_page = response.xpath("//span[contains(@class,'ti-arrow-right')]//parent::a//@href").get()
        if next_page:            
            yield Request(
                    next_page,
                    callback = self.parse)
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.xpath("//strong[contains(.,'Konut Tipi')]//parent::li/text()").get()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
        item_loader.add_value("external_source", "Altinemlak_PySpider_"+ self.country + "_" + self.locale)        
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//div[contains(@class,'price')]//h2/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        external_id=response.xpath("//span[contains(.,'İlan No')]//strong//text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id)

        rent=response.xpath("//div[contains(@class,'price')]//h2//span//text()").extract_first()
        if "TL" in rent:
            rent = rent.strip().split(" ")[0].replace(".","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "TRY")
        
        address=response.xpath("//i[contains(@class,'marker')]//parent::span/text()").extract_first()
        if address:
            city = address.split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
        
        images = [x for x in response.xpath("//div[contains(@class,'slider-for')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        desc = " ".join(response.xpath("//div[contains(@class,'block-wrap')]//span//strong//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//strong[contains(.,'Oda Sayısı')]//parent::li/text()").get()
        if "+" in room_count:
            room1 = room_count.split("+")[0]
            room2= room_count.split("+")[1]
            total_room=int(room1)+int(room2)
            item_loader.add_value("room_count", total_room)

        bathroom_count = response.xpath("//strong[contains(.,'Banyo Sayısı')]//parent::li/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)

        parking = response.xpath("//li[contains(.,'Otopark') or contains(.,'Garaj')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balkon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//li[contains(.,'Teras')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//li[contains(.,'Eşyalı')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(.,'Asansör')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//strong[contains(.,'Bulunduğu Kat')]//parent::li/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        latitude_longitude = response.xpath("//script[contains(.,'lat:')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat:')[1].split(',')[0]
            longitude = latitude_longitude.split('lng:')[1].split(',')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//div[contains(@class,'agent')]//img//following-sibling::h4[1]//span//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email = response.xpath("//div[contains(@class,'agent')]//a[contains(@href,'tel')][contains(.,'E-Posta')]//span//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        
        landlord_phone = response.xpath("//div[contains(@class,'agent')]//a[contains(@href,'tel')][contains(.,'Telefon')]//@href").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split(":")[1].strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        status = response.xpath("//strong[contains(.,'Durumu')]//parent::li/text()[contains(.,'Boş')]").get()
        if status:
            yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and ("daire" in p_type_string.lower() or "residence" in p_type_string.lower() ):
        return "apartment"
    elif p_type_string and ("villa" in p_type_string.lower()):
        return "house"  
    else:
        return None