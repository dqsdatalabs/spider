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
    name = 'abprop_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.abprop.co.uk/property/?wppf_search=to-rent&wppf_orderby=latest&wppf_view=list&wppf_lat=0&wppf_lng=0&wppf_radius=10"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "type":url.get('type')})

    # 1. FOLLOWING
    def parse(self, response):
                
        for item in response.xpath("//div[contains(@class,'property_item')]"):
            follow_url = response.urljoin(item.xpath("./figure/a/@href").get())
            property_type = item.xpath(".//h5//text()").get()
            if "Flat" in property_type or "Apartment" in property_type:
                prop_type = "apartment"
            elif "House" in property_type:
                prop_type = "apartment"
            else: return
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": prop_type})
        
        next_page = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Abprop_Co_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("?p=")[-1])
        title = "".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title))
        
        address = "".join(response.xpath("//div/h3[contains(.,'Features')]//../div/strong[contains(.,'Location')]/following-sibling::text()").getall())
        if address:
            address = address.strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
        
        rent = "".join(response.xpath("//span[contains(@class,'price')]//text()").get())
        if rent:
            price = rent.split("pw")[0].split("£")[1].strip().replace(",","")
            item_loader.add_value("rent", int(float(price))*4)
            item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//div/h3[contains(.,'Features')]//../div/strong[contains(.,'Bedroom')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//div/h3[contains(.,'Features')]//../div/strong[contains(.,'Bathroom')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        desc = " ".join(response.xpath("//div[contains(@class,'property_about')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
        square_meters = response.xpath("//div/h3[contains(.,'Features')]//../div/strong[contains(.,'Area')]/following-sibling::text()").get()
        square_mt = response.xpath("//ul/li[contains(.,'sq ft')]/text()").get()
        if square_meters:
            square_meters = square_meters.split("sq")[0].strip()
            sqm = str(int(int(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        elif square_mt:
                square_meters = square_mt.split("sq")[0].strip().split(" ")[-1]
                item_loader.add_value("square_meters", str(int(int(square_meters)* 0.09290304)))
        elif "sq m" in desc:
            square_meters = desc.split("sq m")[0].split("/")[1].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))
            
        images = [x for x in response.xpath("//div[@class='wppf_image']/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [x for x in response.xpath("//div/h3[contains(.,'Floorplan')]/parent::div//a//@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        energy_label = response.xpath("//div[@class='item']/img[contains(@src,'EE_')]/@src").get()
        if energy_label:
            energy_label = energy_label.split("_")[2].lstrip("0")
            item_loader.add_value("energy_label", energy_label)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        elevator = response.xpath("//ul/li[contains(.,'lift') or contains(.,'Lift') or contains(.,'LIFT')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        furnished = response.xpath("//ul/li[contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        swimming_pool = response.xpath("//ul/li[contains(.,'Pool') or contains(.,'pool') or contains(.,'POOL')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        terrace = response.xpath("//ul/li[contains(.,'terrace') or contains(.,'Terrace') or contains(.,'TERRACE')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//ul/li[contains(.,'parking') or contains(.,'Parking') or contains(.,'PARKING')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        pets_allowed = response.xpath("//ul/li[contains(.,'Pet Friendly')]/text()").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)
        
        balcony = response.xpath("//ul/li[contains(.,'balcon') or contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        item_loader.add_value("landlord_name", "Absolute Property")
        
        phone = response.xpath("//div/h3[contains(.,'Contact')]//parent::div/strong[2]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        
        email = response.xpath("//div/h3[contains(.,'Contact')]//parent::div/strong[3]/text()").get()
        if email:
            item_loader.add_value("landlord_email", email.strip())

        yield item_loader.load_item()