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
import dateparser

class MySpider(Spider):

    name = 'tulipestateagents_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://www.tulipestateagents.co.uk/search.php?address=Lettings&area=&filter=&bedrooms=0&minprice=0&maxprice=0&page=1"]
    
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        for item in response.xpath("//a[@class='more']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
            )
            seen=True
        
        if page ==2 or seen:        
            f_url = response.url.replace(f"page={page-1}", f"page={page}")
            yield Request(f_url, callback=self.parse, meta={"page": page+1})
        
           
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Tulipestateagents_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        
        let_agreed = response.xpath("//span[contains(@class,'sold')]//text()[contains(.,'Let Agreed')]").get()
        if let_agreed:
            return

        prop_type = "".join(response.xpath("//div[contains(@class,'six columns info')]/ul/li//text() | //h3/text()").extract())
        if prop_type and ("apartment" in prop_type.lower() or "flat" in prop_type.lower()):
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "house" in prop_type.lower() and "shared house" not in prop_type.lower():
            item_loader.add_value("property_type", "house")
        elif prop_type and "room" in prop_type.lower():
            item_loader.add_value("property_type", "room")
        elif prop_type and "studio" in prop_type.lower():
            item_loader.add_value("property_type", "studio")
        elif prop_type and "Room" in prop_type.lower():
            item_loader.add_value("property_type", "room")
        else:
            return

        title=response.xpath("//h3/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        rent=response.xpath("//h3[contains(.,'Rent')]/text()").get()
        if rent:
            price=rent.split("Rent:")[1].strip().split(" ")[0]
            item_loader.add_value("rent_string", price)
        
        if item_loader.get_collected_values("property_type")[0] == "room":
            item_loader.add_value("room_count", "1")
        else:
            room_count=response.xpath("//h3/text()[contains(.,'Bedroom')]").get()
            if room_count:
                room_count = room_count.split(" ")[0]
                if room_count.isdigit():
                    item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//ul/li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])
        
        address = response.xpath("//div[@class='title']//h3/text()").get()
        if address:
            item_loader.add_value("address", address)
            city = address.split(",")[-1].strip()
            item_loader.add_value("city", city)
        
            
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(")")[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        desc="".join(response.xpath("//div[contains(@class,'info')]/p/text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}','', desc))
        
        available_date = response.xpath("//div[contains(@class,'info')]/p/text()[contains(.,'Available from')]").get()
        if available_date:
            available_date = available_date.split("Available from")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
                
        energy_label=response.xpath("//div[contains(@class,'info')]/ul/li[contains(.,'Gas')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("/")[0].split(" ")[-1])
        
        furnished=response.xpath("//div[contains(@class,'info')]/ul/li[contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        pets_allowed=response.xpath("//div[contains(@class,'info')]/ul/li[contains(.,'Pet') and not(contains(.,'No'))]/text()").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True) 
        
        images=[x for x in response.xpath("//ul[@class='slides']/li/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name","Tulip Estate Agents")
        item_loader.add_value("landlord_phone","01482 346366")
        item_loader.add_value("landlord_email","contact@tulipestateagents.co.uk")
        
        yield item_loader.load_item()