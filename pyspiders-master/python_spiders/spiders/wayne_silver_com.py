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
    name = 'wayne_silver_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'   
    def start_requests(self):

        formdata = {
            "sortorder": "price-desc",
            "RPP": "12",
            "OrganisationId": "42a411e0-aaa5-4d01-af75-0919f2a3fa00",
            "WebdadiSubTypeName": "Rentals",
            "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c},{59c95297-2dca-4b55-9c10-220a8d1a5bed}",
            "includeSoldButton": "true",
            "page": "1",
            "incsold": "true",
        }

        yield FormRequest(
            url="https://www.wayne-silver.com/api/set/results/grid",
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'property-status to-let')]/../following-sibling::div/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:

            formdata = {
                "sortorder": "price-desc",
                "RPP": "12",
                "OrganisationId": "42a411e0-aaa5-4d01-af75-0919f2a3fa00",
                "WebdadiSubTypeName": "Rentals",
                "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c},{59c95297-2dca-4b55-9c10-220a8d1a5bed}",
                "includeSoldButton": "true",
                "page": str(page),
                "incsold": "true",
            }
            yield FormRequest(
                url="https://www.wayne-silver.com/api/set/results/grid",
                callback=self.parse,
                formdata=formdata,
                meta={"page":page+1}
            )
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)

        p_type = response.url.split("/")[-2]        
        if "studio" in response.url:
            item_loader.add_value("property_type", "studio")
        elif p_type and "studio" in p_type.lower():
            item_loader.add_value("property_type", "studio")
        elif p_type and ("apartment" in p_type.lower() or "flat" in p_type.lower() or "maisonette" in p_type.lower()):
            item_loader.add_value("property_type", "apartment")
        elif p_type and "house" in p_type.lower():
            item_loader.add_value("property_type", "house")      
        elif p_type and "student" in p_type.lower():
            item_loader.add_value("property_type", "student_apartment")
        else:
            return
        item_loader.add_value("external_source", "Wayne_Silver_PySpider_united_kingdom")
        
        title = response.xpath("//section[@id='description']//h2/text()").extract_first()
        if title:
            item_loader.add_value("title", title.strip())

        zipcode = response.xpath("//span[@class='displayPostCode']//text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        city = response.xpath("//span[@class='city']//text()").extract_first()
        if city:
            item_loader.add_value("city", city.replace(",","").strip())

        address ="".join(response.xpath("//div[contains(@class,'property-address')]//text()[normalize-space()]").extract())
        if address:        
            item_loader.add_value("address", address.strip())        
 
        room_count = response.xpath("//ul[@class='FeaturedProperty__list-stats']/li[img[contains(@src,'bed')]]/span//text()[.!='0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif "studio" in response.url:
            item_loader.add_value("room_count", "1")
        
        bathroom_count=response.xpath("//ul[@class='FeaturedProperty__list-stats']/li[img[contains(@src,'bathroom')]]/span//text()[.!='0']").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = " ".join(response.xpath("//div[contains(@class,'property-price')]//text()[normalize-space()][not(contains(.,'Price on'))]").extract())
        if rent:
            if "per month)" in rent:
                rent = rent.split("(")[1]
            item_loader.add_value("rent_string", rent)  
        if not rent:
            item_loader.add_value("currency", "GBP")  

       
        desc = " ".join(response.xpath("//section[@id='description']//p/text()").extract())
        if desc:
            item_loader.add_value("description",desc.strip())
            if "furnished or unfurnished" in desc.lower():
                pass
            elif "unfurnished" in desc.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in desc.lower():
                item_loader.add_value("furnished", True)
            if "parking" in desc:
                item_loader.add_value("parking", True) 
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sq ft|sqft|sqft|sq.ft|sq ft|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",desc.replace(",",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)
                   
        map_coordinate = response.xpath("//section[@id='maps']/@data-cords").extract_first()
        if map_coordinate:
            latitude = map_coordinate.split('"lat": "')[1].split('",')[0].strip()
            longitude = map_coordinate.split('"lng": "')[1].split('"}')[0].strip()
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)

        images = [response.urljoin(x) for x in response.xpath("//div[@id='propertyDetailsGallery']//div/@data-bg").extract()]
        if images:
            item_loader.add_value("images", images)   

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'image-wrapper-floorplan-lightbox')]//img/@data-src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images) 
     
        elevator = response.xpath("//div[@id='collapseOne']//ul/li[contains(.,'Lift') or contains(.,'lift ') ]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True) 
        terrace = response.xpath("//div[@id='collapseOne']//ul/li[contains(.,'Terrace') or contains(.,'terrace') ]").get()
        if terrace:
            item_loader.add_value("terrace", True)  
              
        parking = response.xpath("//div[@id='collapseOne']//ul/li[contains(.,'Garage') or contains(.,'Parking') ]").get()
        if parking:
            item_loader.add_value("parking", True) 

        item_loader.add_value("landlord_phone", "020 7431 4488")
        item_loader.add_value("landlord_email", "info@wayne-silver.com")
        item_loader.add_value("landlord_name", "Wayne & Silver Ltd")  

        yield item_loader.load_item()
