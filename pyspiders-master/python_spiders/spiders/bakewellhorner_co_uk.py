# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n


class MySpider(Spider):
    name = 'bakewellhorner_co_uk'  
    execution_type='testing'
    country='united_kingdom'
    locale='en'        
    thousand_separator = ','
    scale_separator = '.'  
    def start_requests(self):
        start_urls = [
            {"url": "https://www.bakewellhorner.co.uk/Property-Search?location=&Prop_Category=2&Prop_PropType=Flat&Prop_Price_rent_min=&Prop_Price_rent_max=", "property_type": "apartment"},
	        {"url": "https://www.bakewellhorner.co.uk/Property-Search?location=&Prop_Category=2&Prop_PropType=House&Prop_Price_rent_min=&Prop_Price_rent_max=", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
    
        for item in response.xpath("//div[@class='half-result']/div[@class='property-result']/div[@class='pr-image']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source","Bakewellhorner_Co_PySpider_"+ self.country)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("?id=")[-1])
        item_loader.add_xpath("title", "//div[@class='prop-head-desc']/text()")
        rented = response.xpath("//div[@class='prop-head-status']/text()[.='LET AGREED']").get()
        if rented:
            return
        address = response.xpath("//input[@name='propdisplayaddress']/@value").get()
        if address:
            city = address.split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
        
        room_count = response.xpath("//li[contains(.,'Bedroom')]//text()").get()
        room_count2 = response.xpath("//li[contains(.,'bedroom')]//text()").get()
        room2 = response.xpath("//li[contains(.,'Reception')]//text()").get()
        room = False
        if room_count:
            room = room_count.strip().split(" ")[0]
        elif room_count2:
            room = room_count2.strip().split(" ")[0]
        elif room2:
            room = room2.strip().split(" ")[0]
        
        if room:
            try:
                item_loader.add_value("room_count" , w2n.word_to_num(room))
            except: pass
        

        if not item_loader.get_collected_values("room_count"):
            room_control = response.xpath("//div[@class='prop-head-desc']/text()").get()
            for i in room_control.split(","):
                if "bedroom" in i.lower():
                    room_count = i.replace("bedroom","").strip()
                    if room_count != "0":
                        item_loader.add_value("room_count" , room_count)
                        break
            
             
        bathroom = response.xpath("//li[contains(.,'Bathroom') or contains(.,'bathroom')]//text()").get()
        if bathroom:
            try:
                item_loader.add_value("bathroom_count" , w2n.word_to_num(bathroom))
            except: 
                item_loader.add_value("bathroom_count", "1")
            
        rent = response.xpath("//div[@class='prop-head-price']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)
        
        lat_lng = response.xpath("//script[contains(.,'lat')]/text()").get()
        if lat_lng:
            lat = lat_lng.split("lat:")[1].split(",")[0].strip()
            lng = lat_lng.split("lng:")[1].split("}")[0].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
            
        desc = "".join(response.xpath("//div[@class='text']//strong/..//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        images = [ x.replace("background-image:url(","").replace(");","") for x in response.xpath("//div[@class='slide-image']/@style").getall()]
        if images:
            item_loader.add_value("images", images)
        
        energy = response.xpath("//div[contains(@class,'epc')]/a/img/@src[contains(.,'EE')]").get()
        energy_label = response.xpath("//li[contains(.,'EPC Rating')]//text()").get()
        if energy:
            energy = energy.split("_")[-2]
            item_loader.add_value("energy_label", energy_label_calculate(energy))
        elif energy_label:
            energy_label = energy_label.split("EPC Rating")[1].strip()
            if "TBC" not in energy_label:
                item_loader.add_value("energy_label", energy_label)

        floor = response.xpath("//li[contains(.,'floor') or contains(.,'Floor')]//text()").get()
        if floor:
            if "Floor" in floor:
                item_loader.add_value("floor", floor.split("Floor")[0])
            else:
                item_loader.add_value("floor", floor.split("floor")[0])
            
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//li[contains(.,'terrace') or contains(.,'Terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//li[contains(.,'lift') or contains(.,'Lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//li[contains(.,'balcon') or contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        unfurnished = response.xpath("//li[contains(.,'unfurnished') or contains(.,'Unfurnished')]//text()").get()
        furnished = response.xpath("//li[contains(.,'furnished') or contains(.,'Furnished')]//text()").get()
        
        if unfurnished:
            item_loader.add_value("furnished", False)
        elif furnished:
            item_loader.add_value("furnished", True)
            
        pets_allowed = response.xpath("//li[contains(.,'No Pets')]//text()").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", False)
        
                
        item_loader.add_value("landlord_name", "Bakewell & Horner Estate Agents")
        
        phone = response.xpath("//span[@class='call']/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("Call","").strip())

        item_loader.add_value("landlord_email", "info@bakewellhorner.co.uk")
        
        yield item_loader.load_item()
    
def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label