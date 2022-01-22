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
import dateparser
class MySpider(Spider):
    name = 'docklandsresidential_com'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://docklandsresidential.com/properties/?department=residential-lettings&address_keyword=&property_type=22&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&commercial_property_type=&minimum_floor_area=&maximum_floor_area=",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://docklandsresidential.com/properties/?department=residential-lettings&address_keyword=&property_type=9&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&commercial_property_type=&minimum_floor_area=&maximum_floor_area=",
                ],
                "property_type" : "house"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[.='View']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//div[@class='qt-property-info']/div[div[contains(.,'Availability')]]/div[2]/text()").extract_first()     
        if status:   
            if "let agreed" in status.lower():
                return
        property_type = response.meta.get('property_type')
        prop_type = response.xpath("//div[@class='qt-property-info']/div[div[contains(.,'Type')]]/div[2]/text()").extract_first()     
        if prop_type: 
            if "studio" in prop_type.lower():
                property_type = "studio"
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Docklandsresidential_PySpider_"+ self.country + "_" + self.locale)
        title = response.xpath("//div/h1/text()").extract_first()     
        if title:   
            item_loader.add_value("title",title.strip()) 
            address = ""
            if "to let in" in title.lower():
                address = title.lower().split("to let in")[1].strip()
            elif "to let" in title.lower():
                address = title.lower().split("to let")[1].strip()
            elif "house" in title.lower():
                address = title.lower().split("house")[1].strip()
            elif "," in title.lower():
                address = title.lower().split(",")[1].strip()
            elif "flat" in title.lower():
                address = title.lower().split("flat")[1].strip()
            elif "in" in title.lower():
                address = title.lower().split("in")[1].strip()
            else:
                address = title
            address2 = response.xpath("//div[@class='qt-property-single-header']/h3/text()").extract_first() 
            if address2:
                address = address+", "+address2
            item_loader.add_value("address",address.strip().upper()) 
            if address:
                try:          
                    zipcode = ""          
                    if any(map(str.isdigit, address.split(" ")[-2])):
                        zipcode = address.split(" ")[-2]+" "+address.split(" ")[-1]
                    else:
                        zipcode = address.split(" ")[-1]
                    if zipcode and "," not in zipcode:
                        item_loader.add_value("zipcode",zipcode.strip().upper()) 
                except:
                    pass
   
        rent = response.xpath("//div[@class='qt-property-single-header']/h2//text()").extract_first()
        if rent:
            if "pw" in rent.lower():
                numbers = re.findall(r'\d+(?:\.\d+)?', rent.replace(",","."))
                if numbers:
                    rent = int(numbers[0].replace(".",""))*4
                    rent = "£"+str(rent)
            item_loader.add_value("rent_string", rent.replace(",","."))
        room = response.xpath("//div/span[contains(@class,'bedroom')]/text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.replace("x","").strip())
        bathroom_count = response.xpath("//div/span[contains(@class,'bathroom')]/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.replace("x","").strip())
 
        furnished = response.xpath("//div[@class='qt-property-info']/div[div[contains(.,'Furnished')]]/div[2]/text()").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", True)
        
        available_date = response.xpath("//div[@class='qt-property-info']/div[div[contains(.,'Available Date')]]/div[2]/text()[not(contains(.,'Now'))]").extract_first()
        if available_date:  
            try:                
                newformat = dateparser.parse(available_date, languages=['en']).strftime("%Y-%m-%d")
                item_loader.add_value("available_date", newformat)
            except:
                pass
        map_coordinate = response.xpath("//script/text()[contains(.,'map_lat =') and contains(.,'map_lng =')]").extract_first()
        if map_coordinate:
            latitude = map_coordinate.split('map_lat =')[1].split(';')[0].strip()
            longitude = map_coordinate.split('map_lng =')[1].split(';')[0].strip()
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
            
        desc = " ".join(response.xpath("//div/h3[contains(.,'Summary')]/following-sibling::p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
  

        deposit = response.xpath("//div[@class='qt-property-info']/div[div[contains(.,'Deposit')]]/div[2]/text()").extract_first()
        if deposit:
            deposit = deposit.replace(",","").strip()
            item_loader.add_value("deposit", deposit)
        images = [x for x in response.xpath("//div[contains(@class,'single-property-gallery-swipebox-wrapper')]//div/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "0203 841 5697")
        item_loader.add_value("landlord_email", "info@docklandsresidential.com")
        item_loader.add_value("landlord_name", "Docklands Estates")
        yield item_loader.load_item()
