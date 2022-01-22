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
    name = 'sunflowerlettings_co_uk'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'  
    start_urls = ["https://www.sunflowerlettings.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areadata=&areaname=&radius=&bedrooms=&minprice=&maxprice="]

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='mapStatus' and contains(.,'Available')]/../@href").extract():
            follow_url = response.urljoin(item)
            map_url = 'https://www.sunflowerlettings.co.uk/Map-Property-Search-Results?references=' + follow_url.split('/')[-1].strip()
            yield Request(map_url, callback=self.get_latlong, meta={'follow_url': follow_url})

    def get_latlong(self, response):

        data = json.loads(response.body)
        latitude = data['items'][0]['lat']
        longitude = data['items'][0]['lng']
        yield Request(response.meta.get("follow_url"), callback=self.populate_item, meta={'latitude': latitude, 'longitude': longitude}) 
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        
        summary = "".join(response.xpath("//h2[contains(@class,'summaryTitle summaryTitle1')]/following-sibling::div[1]//text()").getall())
        if summary and ("apartment" in summary.lower() or "flat" in summary.lower() or "maisonette" in summary.lower()):
            item_loader.add_value("property_type", "apartment")
        elif summary and "house" in summary.lower():
             item_loader.add_value("property_type", "house")
        elif summary and "studio" in summary.lower():
             item_loader.add_value("property_type", "studio")
        else:
            return
        item_loader.add_value("external_source", "Sunflowerlettings_PySpider_"+ self.country + "_" + self.locale)

        latitude = response.meta.get('latitude')
        if latitude:
            item_loader.add_value("latitude", str(latitude))
        longitude = response.meta.get('longitude')
        if longitude:
            item_loader.add_value("longitude", str(longitude))

        title = response.xpath("//div/h1/text()").extract_first()     
        if title:   
            item_loader.add_value("title",title.strip()) 
            item_loader.add_value("address",title.strip())        
            item_loader.add_value("zipcode",title.split(",")[-1].strip()) 
            
        rent = response.xpath("//div[@class='row']/h2[contains(.,'£')]/div/text()[normalize-space()]").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace(",","."))

        room = response.xpath("//div[@class='detailRoomsIcon']/i[@class='i-bedrooms']/following-sibling::text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.strip())

        # bathroom_count = response.xpath("//div[@class='detailRoomsIcon']/i[@class='i-bathrooms']/following-sibling::text()[normalize-space()]").extract_first()
        # if bathroom_count:
        #     item_loader.add_value("bathroom_count", bathroom_count.strip())
     
        desc = " ".join(response.xpath("//div[@class='detailsTabs']/h2[contains(.,'Description')]/following-sibling::div//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "parking" in desc.lower() or "garage" in desc.lower():
                item_loader.add_value("parking", True)
            if "furnished or unfurnished" in desc.lower():
                pass
            elif "unfurnished" in desc.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in desc.lower():
                item_loader.add_value("furnished", True)
            
            
        images = [x for x in response.xpath("//div[@id='property-photos-device1']/a/@href").extract()]
        if images:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "01732 252 022")
        item_loader.add_value("landlord_email", "info@sunflowerlettings.co.uk")
        item_loader.add_value("landlord_name", "Sunflower Lettings")
        map_id = response.url.split("/")[-1]
        if map_id:
            map_url = f"https://www.sunflowerlettings.co.uk/Map-Property-Search-Results?references={map_id}&category=1"
            yield Request(map_url, callback=self.parse_map,meta={"item":item_loader})
        else:
            yield item_loader.load_item()
        
    def parse_map(self, response):
        item_loader = response.meta.get('item')
        map_json = json.loads(response.body)
       
        for data in map_json["items"]:
            lat = data["lat"]
            lng = data["lng"]
            if lat and lng:
                item_loader.add_value("latitude", str(lat))
                item_loader.add_value("longitude", str(lng))
        
        yield item_loader.load_item()
