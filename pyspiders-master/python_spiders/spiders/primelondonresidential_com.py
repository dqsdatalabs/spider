# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re
from word2number import w2n


class MySpider(Spider):
    name = 'primelondonresidential_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://www.primelondonresidential.com/properties-to-let"]

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='eapow-property-thumb-holder']/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
            )
            
        next_page = response.xpath("//a[@title='Next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
            )

        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Primelondonresidential_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)

        prop_type = "".join(response.xpath("//ul[@id='starItem']//text()").getall())
        if prop_type and ("apartment" in prop_type or "penthouse" in prop_type or "flat" in prop_type):
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "house" in prop_type:
            item_loader.add_value("property_type", "house")
        else:
            return
              
        title = response.xpath("//h1/text()[normalize-space()]").extract_first()
        if title:
            item_loader.add_value("title", title)
            item_loader.add_value("address",title)
        city=response.xpath("//h1/text()[normalize-space()]").extract_first()
        if city:
            city=city.split(",")[-1]
            if city:
                city=re.findall("[^0-9]",city)
                city="".join(city).replace(" ","")
                item_loader.add_value("city",city.strip())

        rent = response.xpath("//small[@class='eapow-detail-price']//text()").get()
        if rent:
            numbers = re.findall(r'\d+(?:\.\d+)?', rent.replace(",","."))
            if numbers:
                rent = int(numbers[0].replace(".",""))*4
                rent = "£"+str(rent)
            item_loader.add_value("rent_string", rent)    
    
        room_count = response.xpath("//div/i[@class='flaticon-bed']/following-sibling::span//text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//i[@class='flaticon-bath']/following-sibling::span/text()").get()
        if bathroom_count: item_loader.add_value("bathroom_count", bathroom_count.strip())
            
        desc = "".join(response.xpath("//div[contains(@class,'eapow-desc-wrapper')]//p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        square_meters = response.xpath("//ul[@id='starItem']/li[contains(.,'sq')]//text()").get()
        if square_meters:
            square_meters=square_meters.replace("(","").replace(")","")
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(sq m|sqm)",square_meters.replace(",","."))
            if unit_pattern:
                sq=int(float(unit_pattern[0][0]))
                item_loader.add_value("square_meters", str(sq)) 
            else:
                unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(sq ft|sqft)",square_meters.replace(",",""))
                if unit_pattern:
                    sqm = str(int(float(unit_pattern[0][0]) * 0.09290304))
                    item_loader.add_value("square_meters", sqm)
        else:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(sq ft|sqft)",desc.replace(",",""))
            if unit_pattern and ('80000' not in unit_pattern[0][0]):
                sqm = str(int(float(unit_pattern[0][0]) * 0.09290304))
                item_loader.add_value("square_meters", sqm)

        swimming_pool = response.xpath("//ul[@id='starItem']/li[contains(.,'swimming pool')]//text()").extract_first()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)       

        coordinate = response.xpath("//script[@type='text/javascript'][contains(.,'eapowmapoptions')]//text()").extract_first()
        if coordinate:
            latitude = coordinate.split('lat: "')[1].split('"')[0].strip()
            longitude = coordinate.split('lon: "')[1].split('"')[0].strip()        
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        balcony = response.xpath("//ul[@id='starItem']/li[contains(.,'balcon') or contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)   

        furnished = response.xpath("//ul[@id='starItem']/li[contains(.,'furnished') and not(contains(.,'Unfurnished'))]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True) 

        parking = response.xpath("//ul[@id='starItem']/li[contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True) 

        terrace = response.xpath("//ul[@id='starItem']/li[contains(.,'terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True) 
      
        images = [x for x in response.xpath("//div[@id='eapowgalleryplug']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        floor_plan_images = [x for x in response.xpath("//div[@id='eapowfloorplanplug']//img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
            
        item_loader.add_value("landlord_email", "Office@PrimeLondonResidential.com")
        item_loader.add_value("landlord_name", "Prime London Residential")
        item_loader.add_value("landlord_phone", "0207 928 6663")
        yield item_loader.load_item()

