# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek 

from itemloaders import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re
class MySpider(Spider):

    name = 'victorstone_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = "Victorstone_PySpider_united_kingdom_en"
    thousand_separator = ','
    scale_separator = '.'
    custom_settings = {
    "HTTPCACHE_ENABLED": False,
    } 
    start_urls = ["https://www.victorstone.co.uk/Search?listingType=6&areadata=&areaname=&minprice=&maxprice=&statusids=2&statusids=1&obc=Price&obd=Ascending&page=1"]
    
    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//a[.='Full Details']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
            )
            seen = True
        
        if  page == 2 or seen:
            p_url = f"https://www.victorstone.co.uk/Search?listingType=6&areadata=&areaname=&minprice=&maxprice=&statusids=2&statusids=1&obc=Price&obd=Ascending&page={page}"
            yield Request(
                p_url, 
                callback=self.parse,
                meta={"page":page+1} 
            )
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        prop_type = "".join(response.xpath("//div[contains(@class,'mainDescWrapper')]/text()").extract())
        if prop_type and ("apartment" in prop_type.lower() or "flat" in prop_type.lower()):
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "house" in prop_type.lower():
            item_loader.add_value("property_type", "house")
        elif prop_type and "room" in prop_type.lower():
            item_loader.add_value("property_type", "room")
        elif prop_type and "studio" in prop_type.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", "apartment")

        
        item_loader.add_value("external_source", self.external_source)
        let_agreed = response.xpath("//div[@class='status']//text()[contains(.,'Let Agreed')]").extract_first()
        if let_agreed:
            return
        externalid=response.url
        if externalid:
            externalid=externalid.split("/")[-1]
            item_loader.add_value("external_id",externalid)
            
        item_loader.add_xpath("title", "//title//text()")
        address = response.xpath("//div//h1//text()").extract_first()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(",")[-1]
            if zipcode:
                item_loader.add_value("zipcode", zipcode.strip())
        city=response.xpath("//div//h1//text()").extract_first()
        if city:
            city=city.split(",")[-2]
            item_loader.add_value("city", city)


        rent ="".join(response.xpath("//div[contains(@class,'detailSliderText')]//h2/div[contains(.,'£')]//text()").extract())
        if rent:
            if "PCM" in rent:
                numbers = re.findall(r'\d+(?:\.\d+)?', rent.replace(",","."))
                if numbers:
                    rent = int(numbers[0].replace(".",""))
                    rent = "£"+str(rent)
                item_loader.add_value("rent_string", rent)
            else:
                numbers = re.findall(r'\d+(?:\.\d+)?', rent.replace(",","."))
                if numbers:
                    rent = int(numbers[0].replace(".",""))*4
                    rent = "£"+str(rent)
                item_loader.add_value("rent_string", rent)   
    

     
        room = response.xpath("//div[@class='rooms' and contains(.,'Bedroom') and not(contains(.,'0 Bedroom'))]//text()[normalize-space()]").extract_first()
        if room:
            room = room.split("Bedroom")[0].strip()
            item_loader.add_value("room_count", room)
            if room == "-":
                room = response.xpath("//i[@class='vroom-sofa']/following-sibling::text()[normalize-space()]").extract_first()
                if room:
                    item_loader.add_value("room_count",room.split("Receptions")[0].strip())
            
        else:
            room = response.xpath("//i[@class='vroom-sofa']/following-sibling::text()[normalize-space()]").extract_first()
            if room:
                item_loader.add_value("room_count",room.split("Receptions")[0].strip())



        bathroom = response.xpath("//div[@class='rooms' and contains(.,'Bathroom') and not(contains(.,'0 Bathroom'))]//text()[normalize-space()]").extract_first()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.split("Bathroom")[0].strip())
            
        desc = "".join(response.xpath("//div[@class='row mainDescWrapper']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        terrace = response.xpath("//ul//li[contains(.,'Terrace')]//text()").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)

        floor = response.xpath("//ul//li[contains(.,'Floor')]//text()[not(contains(.,'Wood') or contains(.,'Floor Plan') )]").extract_first()
        if floor:
            item_loader.add_value("floor", floor.split("Floor")[0])

        elevator = response.xpath("//ul//li[contains(.,'Lift')]//text()").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)
        
        furnished = response.xpath("//ul//li[contains(.,'Furnished')]//text()").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)
       
        balcony = response.xpath("//ul//li[contains(.,'Balcony')]//text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
    
        images = [x for x in response.xpath("//div[@id='property-photos-device1']//a/@href").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "0207 8581588")
        item_loader.add_value("landlord_name", "Victorstone")
        item_loader.add_value("landlord_email", "city@victorstone.co.uk")


        position_url = f"https://www.victorstone.co.uk/Map-Property-Search-Results?references={externalid}"

            
        yield Request(
                position_url, 
                callback=self.take_position,
                meta={"item_loader":item_loader} 
            )

    def take_position(self,response):
        item_loader = response.meta.get("item_loader")
        data = json.loads(response.body.decode())["items"][0]
        lat = data["lat"]
        long = data["lng"]
        item_loader.add_value("latitude",str(lat))
        item_loader.add_value("longitude",str(long).replace("-",""))


        yield item_loader.load_item()