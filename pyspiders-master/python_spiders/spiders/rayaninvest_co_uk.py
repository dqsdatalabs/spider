# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'rayaninvest_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Rayaninvest_Co_PySpider_united_kingdom'
    def start_requests(self):
        headers = {
            "content-type": "application/x-www-form-urlencoded",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "origin": "http://rayaninvest.co.uk"
        }
       
        data = {
            "isSale": "false",
            "minPrice": "0",
            "maxPrice": "0",
            "location": "",
            "minBedrooms": "1",
       }
       
        url = "http://rayaninvest.co.uk/PropertySearch/Residential"        
        yield FormRequest(
            url,
            formdata=data,
            headers=headers,
            callback=self.parse,
            meta={"property_type":"apartment"},
        )
    
    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
 
        for item in response.xpath("//div[@class='col-md-9']/div[@class='row']//h3/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//div[@class='product-info']/h3/text()")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        
        address = response.xpath("//div[@class='product-info']/h3/text()").get()
        if address:
            city = address.replace("United Kingdom","").replace("United kingdom","").strip().strip(",").split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
        
        rent = response.xpath("//span[@class='period']//text()").get()
        if "week" in rent and rent:
            price = response.xpath("//span[@class='price']//text()").get()
            if price:
                price = int(price.replace("£","").replace(",",""))*4
                item_loader.add_value("rent", str(price) )
        elif "month" in rent and rent:
            price = response.xpath("//span[@class='price']//text()").get()
            item_loader.add_value("rent", price.replace("£","").replace(",","") )
        
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//li[contains(.,'Bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        
        bathroom_count = response.xpath("//li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])
        
        external_id = response.xpath("//td[contains(.,'Property ID')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        garage = response.xpath("//td[contains(.,'Garage')]/text()").get()
        parking = response.xpath("//td[contains(.,'Parking')]/text()").get()
        if parking:
            if parking.strip() != "0":
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)
        elif garage:
            if garage.strip() != "0":
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)
            
        desc = " ".join(response.xpath("//div[@class='tab-body']//p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        desc2 = desc.lower().replace("."," ")
        if "sq ft" in desc2:
            square_meters = desc2.split("sq ft")[0].strip().split(" ")[-1].replace("(","").replace(",","")
            sqm = str(int(int(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        
        images = [ x for x in response.xpath("//div[@class='product-gallery']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = response.urljoin(response.xpath("//div[contains(@class,'floorplan')]//a/@href").get())
        if floor_plan_images and "Picture" in floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        energy = response.xpath("//div[contains(@class,'tab-body')]//img/@src[contains(.,'EER')]").get()
        if energy:
            energy_label = energy.split("Current=")[1].split("&")[0]
            item_loader.add_value("energy_label", energy_label_calculate(energy_label))
        
        lat_lng = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if lat_lng:
            lat = lat_lng.split("LatLng(")[1].split(",")[0]
            lng = lat_lng.split("LatLng(")[1].split(",")[1].split(")")[0].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        
        balcony = response.xpath("//div[@class='tab-body']//p//text()[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        furnished = response.xpath("//div[@class='tab-body']//p//text()[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//div[@class='tab-body']//p//text()[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//div[@class='tab-body']//p//text()[contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        item_loader.add_value("landlord_name","Rayan Investments & Management")
        item_loader.add_value("landlord_phone","020 7402 4030")
        
        
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