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
    name = 'chilternproperty_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://chilternproperty.com/listings.php?sale=2&prop_sub=7,8,9,10,11,28,29,44,56,59,142,143,144&status=0&status=3&order_by=price%20desc&list=1", "property_type": "apartment"},
	        {"url": "https://chilternproperty.com/listings.php?sale=2&prop_sub=1,2,3,4,5,6,16,20,21,22,23,24,26,27,30,43,46,47,50,52,53,62,65,68,71,74,77,92,95,101,104,107,110,113,116,117,118,119,120,121,125,128,131,140,141&status=0&status=3&order_by=price%20desc&list=1", "property_type": "house"},
            {"url": "https://chilternproperty.com/listings.php?sale=2&prop_sub=12,13,14,15&status=0&status=3&order_by=price%20desc&list=1", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for item in response.xpath("//div[@id='listings_wrapper']/div[@id='property_wrapper']//p[@class='display_address']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
 
        pagination = response.xpath("//div[@class='pagin'][1]/a[contains(.,'Next')]/@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source","Chilternproperty_PySpider_"+ self.country)
        item_loader.add_xpath("title", "//title[1]/text()")
        item_loader.add_value("external_link", response.url)
        
        address = response.xpath("//div[@class='display_address']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            if "," in address:
                city = address.split(",")[-1]
            else:
                city = address
            item_loader.add_value("city", city.replace("\r\n","").strip())

        rent = response.xpath("//div[@class='display_price']/text()").get()
        if address:
            if "pw" in rent:
                price = rent.split("pw")[0].split("£")[1].replace(",","").strip()
                item_loader.add_value("rent", str(int(price)*4))
            elif "pcm" in rent:
                price = rent.split("pcm")[0].split("£")[1].replace(",","").strip()
                item_loader.add_value("rent", price)
                
        item_loader.add_value("currency", "GBP")

        room_count = response.xpath("//div/p[contains(.,'Bedroom')]/text()").get()
        room = response.xpath("//div/p[contains(.,'Reception')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        elif room:
            item_loader.add_value("room_count", room.strip().split(" ")[0])
            
        bathroom_count = response.xpath("//div/p[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])
        
        desc = "".join(response.xpath("//div[@id='details_description_wrapper']//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        lat_lng = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if lat_lng:
            lat = lat_lng.split("LatLng(")[1].split(",")[0]
            lng = lat_lng.split("LatLng(")[1].split(",")[1].split(")")[0].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        
        energy_label = response.xpath("//div[@id='details_description_wrapper']//text()[contains(.,'EPC')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("-")[1].strip())
        
        furnished = response.xpath("//div[@id='inner']//text()[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        parking = response.xpath(
            "//ul/li[contains(.,'parking') or contains(.,'Parking') or contains(.,'garage') or contains(.,'Garage')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//ul/li[contains(.,'Lift') or contains(.,'lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//ul/li[contains(.,'balcon') or contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//ul/li[contains(.,'terrace') or contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        washing = response.xpath("//ul/li[contains(.,'Washing Machine')]/text()").get()
        if washing:
            item_loader.add_value("washing_machine", True)
        
        images = [ x for x in response.xpath("//ul[@class='slides']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = response.xpath("//div[contains(@id,'floorplan')]/img/@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        item_loader.add_value("landlord_name", "CHILTERNHILLS")
    
        phone = response.xpath("//p/img[contains(@src,'contact')]/parent::p/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        
        item_loader.add_value("landlord_email", "info@chilternproperty.com")
        
        
        yield item_loader.load_item()