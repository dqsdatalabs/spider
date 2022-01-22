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
from datetime import datetime

class MySpider(Spider):
    name = 'amhlettingsandmanagement_co_uk'  
    execution_type='testing'
    country='united_kingdom'
    locale='en'  
    start_urls = ["https://www.amhlettingsandmanagement.co.uk/Map-Property-Search-Results?listingType=6&category=1&statusids=1&obc=Price&obd=Descending&areadata=&areaname=&radius=&bedrooms=&minprice=&maxprice="]


    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data["items"]:
            if item["icontext"] != "Available":
                continue
            follow_url = "https://www.amhlettingsandmanagement.co.uk/property/residential/for-rent/" + item["id"]
            lat, lng = item["lat"], item["lng"]
            yield Request(follow_url, callback=self.populate_item, meta={'lat': lat, "lng": lng})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("latitude",str(response.meta.get("lat")) )
        item_loader.add_value("longitude", str(response.meta.get("lng")))

        summary = "".join(response.xpath("//h2[contains(@class,'summaryTitle1')]/../div/p/text()").getall())
        if summary and "studio" in summary.lower():
             item_loader.add_value("property_type", "studio")
        elif summary and ("apartment" in summary.lower() or "flat" in summary.lower() or "maisonette" in summary.lower()):
            item_loader.add_value("property_type", "apartment")
        elif summary and "house" in summary.lower():
             item_loader.add_value("property_type", "house")
        else:
            return
        item_loader.add_value("external_source", "Amhlettingsandmanagement_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//div/h1/text()").extract_first()     
        if title:   
            item_loader.add_value("title",title.strip()) 
            item_loader.add_value("address",title.strip())        
            item_loader.add_value("city",title.split(",")[1].strip()) 
            item_loader.add_value("zipcode",title.split(",")[-1].strip()) 
            
        rent = response.xpath("//div[@class='row']/h2[contains(.,'£')]/div/text()[normalize-space()]").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace(",","."))

        room = response.xpath("//div[@class='detailRoomsIcon']/i[@class='i-bedrooms']/following-sibling::text()").extract_first()
        if room:
            if room.strip() !="0":
                item_loader.add_value("room_count", room.strip())
            elif summary and "studio" in summary.lower():
                item_loader.add_value("room_count", "1")

        bathroom_count = response.xpath("//div[@class='detailRoomsIcon']/i[@class='i-bathrooms']/following-sibling::text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        furnished = response.xpath("//div[contains(@class,'featuresBox')]/ul/li[contains(.,'Furnished') or contains(.,'furnished')]//text()").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        balcony = response.xpath("//div[contains(@class,'featuresBox')]/ul/li[contains(.,'Balcony')]//text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
            
        terrace = response.xpath("//div[contains(@class,'featuresBox')]/ul/li[contains(.,'terrace')]//text()").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)

        floor = response.xpath("//div[contains(@class,'featuresBox')]/ul/li[contains(.,'floor')]//text()").extract_first()
        if floor:
            if "wooden" in floor.lower():
                pass
            else:
                floor = floor.split("floor")[0]
                item_loader.add_value("floor", floor)
     
        desc = " ".join(response.xpath("//div[@class='detailsTabs']/h2[contains(.,'Summary')]/following-sibling::div//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "furnished or unfurnished" in desc.lower():
                pass
            elif "unfurnished" in desc.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in desc.lower():
                item_loader.add_value("furnished", True)
            if "Available now" in desc:
                available_date = datetime.now()
                item_loader.add_value("available_date", available_date.strftime("%Y-%m-%d"))
                
        images = [x for x in response.xpath("//div[@id='property-photos-device1']/a/@href").extract()]
        if images:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "0203 859 0634")
        item_loader.add_value("landlord_email", "info@amhlettingsandmanagement.co.uk")
        item_loader.add_value("landlord_name", "AMH LETTINGS AND MANAGEMENT")
        yield item_loader.load_item()
