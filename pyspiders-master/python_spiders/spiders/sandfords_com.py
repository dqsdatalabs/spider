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
    name = 'sandfords_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.sandfords.com/property-lettings/flats-available-to-rent-in-london", "property_type": "apartment"},
	        {"url": "https://www.sandfords.com/property-lettings/houses-available-to-rent-in-london", "property_type": "house"},
            {"url": "https://www.sandfords.com/property-lettings/maisonettes-available-to-rent-in-london", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 2)
        if not response.xpath("//div[contains(@class,'no-result')]").get():

            for item in response.xpath("//section[@class='search-posts']/article//h2/a/@href").extract():
                follow_url = response.urljoin(item)
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
            
            base_url = response.meta.get("base_url",response.url)
            url = base_url + f"/page-{page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type":property_type, "base_url":base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source","Sandfords_PySpider_"+ self.country)
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")
        
        address = response.xpath("//div[@class='property-area']/text()").get()
        if address:
            zipcode = address.split(",")[-1].strip()
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            
        rent = response.xpath("//span[@class='price-qualifier']//text()").get()
        if rent:
            price = rent.split("£")[1].replace(",","").strip()
            item_loader.add_value("rent", str(int(price)*4))
        
        item_loader.add_value("currency", "GBP")
        
        room = response.xpath("//h1/text()[contains(.,'bedroom')]").get()
        room2 = response.xpath("//script[contains(.,'dimension') and contains(.,'reception')]").get()
        if room:
            item_loader.add_value("room_count", room.split(" ")[0].strip())
        elif room2:
            room_count = room2.split("reception")[0].split("'")[-1].strip()
            item_loader.add_value("room_count", room_count.split(" ")[0].strip())
            
        
        bathroom = response.xpath("//script[contains(.,'dimension') and contains(.,'bathroom')]").get()
        if bathroom:
            bathroom_count = bathroom.split("bathroom")[0].split("'")[-1].strip()
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = "".join(response.xpath("//div[@class='property-content']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

            if "parking" in desc.lower() or "garage" in desc.lower():
                item_loader.add_value("parking", True)
            
            if "terrace" in desc.lower():
                item_loader.add_value("terrace", True)
        
        if "SHORT LET" in desc:
            return
        
        # if "sq ft" in desc.lower():
        #     square_meters = desc.lower().split("sq ft")[0].strip().split(" ")[-1].replace(",","")
        #     sqm = str(int(int(square_meters)* 0.09290304))
        #     item_loader.add_value("square_meters", sqm)
        
        images = [ x for x in response.xpath("//div[@class='slide']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = response.xpath("//div[contains(@class,'floorplan')]//img/@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        name = response.xpath("//div[@class='title']/text()").get()
        if name:
            item_loader.add_value("landlord_name", name)
        
        phone = response.xpath("//span[@class='track_number']/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("+",""))
        

        latlng_data = response.xpath("//script[contains(.,'startekDetailsMap')]/text()").get()
        if latlng_data:
            lat = latlng_data.split("(jQuery,")[1].split(",")[0].strip().replace("\"","")
            lng = latlng_data.split("(jQuery,")[1].split(",")[1].split(",")[0].strip().replace("\"","")
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)


        yield item_loader.load_item()