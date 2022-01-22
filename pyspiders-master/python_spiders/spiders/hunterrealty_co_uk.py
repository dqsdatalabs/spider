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
    name = 'hunterrealty_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.' 
    start_urls = ["https://hunterrealty.co.uk/property-list.htm?propind=L"]
    custom_settings = {"PROXY_TR_ON": True}

    # 1. FOLLOWING
    def parse(self, response):
        headers = {
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'accept-language': 'en', 
                    'accept-encoding': 'gzip, deflate'
                    }
        for item in response.xpath("//a[contains(.,'More Details')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, headers=headers, callback=self.populate_item)
        
        next_page = response.xpath("//a[.='Next']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//div[@class='status']/img/@src").get()
        if status and "let.png" in status:
            return  
        
        item_loader.add_value("external_link", response.url)

        p_type = "".join(response.xpath("//div[contains(@class,'right col-xs-12 col-sm-8')]/text()").getall())
        if p_type and ("apartment" in p_type.lower() or "flat" in p_type.lower() or "maisonette" in p_type.lower()):
            item_loader.add_value("property_type", "apartment")
        elif p_type and "house" in p_type.lower():
             item_loader.add_value("property_type", "house")
        elif p_type and "studio" in p_type.lower():
             item_loader.add_value("property_type", "studio")
        elif p_type and "room" in p_type.lower():
             item_loader.add_value("property_type", "room")
        elif p_type and "students" in p_type.lower():
             item_loader.add_value("property_type", "student_apartment")
        else:
            return

        
        item_loader.add_value("external_source", "Hunterrealty_Co_PySpider_united_kingdom")
        
        title = response.xpath("//div/h1//text()").extract_first()
        if title:          
            item_loader.add_value("title", title.strip())
            item_loader.add_value("address", title.strip())
            try:
                if "," in title:
                    city = title.split(",")[-1].strip()
                    if city:
                        item_loader.add_value("city",city)
                    else:
                        item_loader.add_value("city",title.split(",")[-2].strip())
                else:
                    item_loader.add_value("city", title.strip())
            except:
                pass
   
        room_count = response.xpath("//div[@id='propertyHeader']/div[contains(.,'bedroom')]").get()
        if room_count and "0 bedroom" not in room_count.strip():
            item_loader.add_value("room_count", room_count.split("bedroom")[0].split("|")[1].strip())     
        elif p_type and "studio" in p_type.lower() or p_type and "room" in p_type.lower():
            item_loader.add_value("room_count", "1")

        rent = " ".join(response.xpath("//div[@id='propertyHeader']//span[contains(@class,'displayprice')]//text()").extract())
        if rent:    
            if "pw" in rent:
                rent = rent.split('£')[1].split('pw')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            else:
                item_loader.add_value("rent_string", rent)    
     
        desc = " ".join(response.xpath("//div[@id='propertyDesc']/text()").extract())
        if desc:
            item_loader.add_value("description",desc.strip())      
            if "unfurnished" in desc.lower():
                item_loader.add_value("furnished", False)  
            elif "furnished" in desc.lower():
                item_loader.add_value("furnished", True) 
            if "parking" in desc.lower():
                item_loader.add_value("parking", True)       
            if "available from" in desc.lower():
                try:
                    date_parsed = dateparser.parse(desc.lower().split("available from")[1].split(".")[0], languages=['en'])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)      
                except:
                    pass
       
        images = [response.urljoin(x) for x in response.xpath("//div[@id='propertySlideshow']//div[@class='propertyimagelist']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)   

        lat = response.xpath("//input[@id='pl_map_lat']/@value").extract_first()
        lng = response.xpath("//input[@id='pl_map_lng']/@value").extract_first()
        if lat and lng:
            item_loader.add_value("longitude", lng)
            item_loader.add_value("latitude", lat)

        item_loader.add_value("landlord_email", "info@hunterrealty.co.uk")
        item_loader.add_value("landlord_phone", "020 8457 9286")
        item_loader.add_value("landlord_name", "Hunter Realty")
        
        yield item_loader.load_item()