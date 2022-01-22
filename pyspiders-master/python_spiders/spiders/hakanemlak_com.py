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


class MySpider(Spider):
    name = 'hakanemlak_com'
    execution_type = 'testing'
    country = 'turkey'
    locale = 'tr' # LEVEL 1
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.hakanemlak.com.tr/emlak-arama/?status=kiralik&location=kastamonu&child-location=any&type=dublex&min-price=any&max-price=any&keyword=",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.hakanemlak.com.tr/emlak-arama/?status=kiralik&location=kastamonu&child-location=any&type=daire&min-price=any&max-price=any&keyword=",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.hakanemlak.com.tr/emlak-arama/?status=kiralik&location=kastamonu&child-location=any&type=mustakil-ev&min-price=any&max-price=any&keyword=",
                "property_type" : "house"
            },
            {
                "url" : "https://www.hakanemlak.com.tr/emlak-arama/?status=kiralik&location=kastamonu&child-location=any&type=villa-konut&min-price=any&max-price=any&keyword=",
                "property_type" : "house"
            },
            {
                "url" : "https://www.hakanemlak.com.tr/emlak-arama/?status=kiralik&location=kastamonu&child-location=any&type=yazlik&min-price=any&max-price=any&keyword=",
                "property_type" : "house"
            },
            {
                "url" : "https://www.hakanemlak.com.tr/emlak-arama/?status=kiralik&location=kastamonu&child-location=any&type=bina&min-price=any&max-price=any&keyword=",
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//h3[@class='entry-title']/a/@href").extract():
            yield Request(item, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'), "city": "Kastamonu"})
        
        next_page = response.xpath("//a[@class='next page-numbers']/@href").get()
        if next_page:
            yield Request(
                url=next_page,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        room=response.xpath("//div[@class='single-property-wrapper']//div[contains(.,'Oda Sayısı')]/span[2]/text()").extract_first()

        item_loader.add_value("title", response.xpath("//h1/text()").get())
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_source", "Hakanemlak_PySpider_"+ self.country + "_" + self.locale)
        city = response.meta.get("city")
        item_loader.add_value("city", city)

        
        item_loader.add_xpath("external_id", "//div[@class='single-property-wrapper']//div[contains(.,'İlan Numarası')]/span[2]/text()")
        item_loader.add_xpath("square_meters", "//div[@class='single-property-wrapper']//div[contains(.,'Genişlik')]/span[2]/text()")
        
        
        latitude_longitude = response.xpath("//script[contains(.,'lang')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat":"')[1].split('"')[0]
            longitude = latitude_longitude.split('lang":"')[1].split('"')[0]
            
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        address = "".join(response.xpath("//div[@class='property-content']/p[contains(.,'LOKASYON')]/following-sibling::p[1]/text()").extract())
        if address:
            item_loader.add_value("address",address)
        if not address:
            item_loader.add_value("address",response.meta.get("city"))



        description = " ".join(response.xpath("//div[@class='property-content']//text()[normalize-space()]").extract())
        if description:
            item_loader.add_value("description", description)
        
        if "teras" in description.lower():
            item_loader.add_value("terrace", True)
        
        if "havuz" in description.lower():
            item_loader.add_value("swimming_pool", True)
        
        price = response.xpath("//div[@class='single-property-wrapper']//span[contains(@class,'price')]").extract_first()
        if price:
            item_loader.add_value("rent", price.strip("TL"))
            item_loader.add_value("currency", "TRY")

    
        if room is not None:
            add=0
            room_array=room.split("+")
            for i in room_array:
                add += int(math.floor(float(i)))
            item_loader.add_value("room_count",str(add) )

        bathroom=response.xpath("//div[@class='single-property-wrapper']//div[contains(.,'Banyo')]/span[2]/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
        
        floor_value=response.xpath("//div[@class='single-property-wrapper']//div[contains(.,'Kat Bilgisi')]/span[2]/text()").extract_first()
        if floor_value is not None:
            if "/" in floor_value:
                floor=floor_value.split("/")[1]
                item_loader.add_value("floor",floor)
            elif "\\" in floor_value:
                print(response.url)
                floor=floor_value.split("\\")[1]
                item_loader.add_value("floor",floor)
    
        images = [response.urljoin(x)for x in response.xpath("//a[@class='swipebox']/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
                
        balcony=response.xpath("//div[@class='property-features']//a[contains(.,'Balkon')]").extract_first()
        if balcony:
            item_loader.add_value("balcony",True)
    
        elevator=response.xpath("//div[@class='property-features']//a[contains(.,'Asansör')]").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)

        parking = response.xpath("//div[@class='property-features']//a[contains(.,'Otopark')]").get()
        if parking:
            item_loader.add_value("parking", True)

    
        furnished=response.xpath("//div[@class='property-features']//a[contains(.,'Mobilya')]").extract_first()
        if furnished is not None:
            item_loader.add_value("furnished",True)
        else:
            furnished_desc=response.xpath("//div[@class='property-content']//li[contains(.,'Eşya')]").extract_first()
            if furnished_desc is not None:
                item_loader.add_value("furnished",True)

        machine =response.xpath("//div[@class='property-features']//a[contains(.,'Beyaz Eşya')]").extract_first()
        if machine is not None:
            item_loader.add_value("washing_machine",True)
            item_loader.add_value("dishwasher",True)
                
        item_loader.add_value("landlord_phone", "0 366 212 7599")
        item_loader.add_xpath("landlord_name", "//h3[@class='agent-name']/a/text()")
        item_loader.add_value("landlord_email", "bilgi@hakanemlak.com.tr")

        yield item_loader.load_item()