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
    name = 'dendrow_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://dendrow.com/property-search/?department=residential-lettings&address_keyword=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=40&submitted=&post_type=property", "property_type": "apartment"},
	        {"url": "https://dendrow.com/property-search/?department=residential-lettings&address_keyword=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=27&submitted=&post_type=property", "property_type": "house"},
            {"url": "https://dendrow.com/property-search/?department=residential-lettings&address_keyword=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=36&submitted=&post_type=property", "property_type": "house"},
            {"url": "https://dendrow.com/property-search/?department=residential-lettings&address_keyword=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=44&submitted=&post_type=property", "property_type": "studio"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for follow_url in response.xpath("//ul[contains(@class,'properties')]/li//h3/a/@href").extract():
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
        
        pagination = response.xpath("//ul[@class='page-numbers']/li/a[contains(@class,'next')]/@href").get()
        if pagination:
            yield Request(pagination, callback=self.parse, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "normalize-space(//div[@class='title']/text())")
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Dendrow_PySpider_united_kingdom")
        address = response.xpath("normalize-space(//div[@class='title']/text())").extract_first()     
        if address:   
            item_loader.add_value("address",address.strip())
            try:
                zipcode = address.split(",")[-1].strip() 
                city = address.split(",")[-2].strip() 
                if "london" in zipcode.lower():
                    zipcode = ""
                    city = address.split(",")[-1].strip()
                if city:
                    item_loader.add_value("city",city.strip())
                if zipcode:
                    item_loader.add_value("zipcode",zipcode.strip())
            except:
                pass
        rent = response.xpath("//div[@class='price']/text()[normalize-space()]").extract_first()
        if rent:
            if "pw" in rent.lower():
                rent_pw = re.findall(r'\d+(?:\.\d+)?', rent.replace(",","."))
                if rent_pw:
                    rent = int(rent_pw[0].replace(".",""))*4
                    if rent > 200000:
                        return
                    rent = "£"+str(rent)
            
            item_loader.add_value("rent_string", rent.replace(",","."))   
            
        desc = " ".join(response.xpath("//div[@class='features']//p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",desc.replace(",","").replace("(",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)

        room_count = response.xpath("//div[@class='rooms']//li[contains(.,'Bedroom')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Bedroom")[0])
   
        bathroom_count=response.xpath("//div[@class='rooms']//li[contains(.,'Bathroom')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Bathroom")[0])

        parking = response.xpath("//div[@class='tablet-portrait-hide']/div[@class='features']//li[contains(.,'parking') or contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)    
     
        elevator = response.xpath("//div[@class='tablet-portrait-hide']/div[@class='features']//li[contains(.,'Lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        balcony = response.xpath("//div[@class='tablet-portrait-hide']/div[@class='features']//li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        terrace = response.xpath("//div[@class='tablet-portrait-hide']/div[@class='features']//li[contains(.,'Terrace') or contains(.,'terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        furnished = response.xpath("//div[@class='tablet-portrait-hide']/div[@class='features']//li[contains(.,'Furnished') or contains(.,'furnished')]//text()").get()
        if furnished:
            if "furnished or unfurnished" in furnished.lower():
                pass
            elif "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        pets_allowed = response.xpath("//div[@class='tablet-portrait-hide']/div[@class='features']//li[contains(.,'Pet Friendly')]//text()").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)

        floor = response.xpath("//div[@class='tablet-portrait-hide']/div[@class='features']//li[contains(.,'Floor') or contains(.,'floor') ]//text()[not(contains(.,'each'))]").get()
        if floor:
            item_loader.add_value("floor", floor.lower().split("floor")[0].strip())
 
        images = [x for x in response.xpath("//div[@id='cycle-1']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)   

        floor_plan_images = [x for x in response.xpath("//li[contains(.,'FLOORPLAN')]/a/@href").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)  

        item_loader.add_value("landlord_phone", "020 7402 3668")
        item_loader.add_value("landlord_email", "info@dendrow.com")
        item_loader.add_value("landlord_name", "DENDROW INTERNATIONAL")

        yield item_loader.load_item()