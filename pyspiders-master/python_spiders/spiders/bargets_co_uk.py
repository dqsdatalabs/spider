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
    name = 'bargets_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://bargets.co.uk/property/page/{}/?department=residential-lettings&property_type=89&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&orderby=",
                    "https://bargets.co.uk/property/page/{}/?department=residential-lettings&property_type=91&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&orderby=",
                    "https://bargets.co.uk/property/page/{}/?department=residential-lettings&property_type=92&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&orderby=",
                    "https://bargets.co.uk/property/page/{}/?department=residential-lettings&property_type=22&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&orderby=",
                    "https://bargets.co.uk/property/page/{}/?department=residential-lettings&property_type=87&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&orderby=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://bargets.co.uk/property/page/{}/?department=residential-lettings&property_type=90&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&orderby=",
                ],
                "property_type" : "studio",
            },
            {
                "url" : [
                    "https://bargets.co.uk/property/page/{}/?department=residential-lettings&property_type=80&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&orderby=",
                    "https://bargets.co.uk/property/page/{}/?department=residential-lettings&property_type=93&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&orderby=",
                    "https://bargets.co.uk/property/page/{}/?department=residential-lettings&property_type=94&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&orderby=",
                    "https://bargets.co.uk/property/page/{}/?department=residential-lettings&property_type=88&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&orderby=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'thumbnail')]/a"):
            status = item.xpath("./div/text()").get()
            if status and ("agreed" in status.lower() or status.strip().lower() == "let"):
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})  
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Bargets_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//h1[@class='property__title']/text()")        
        address = response.xpath("//h1[@class='property__title']/text()").extract_first()
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.split(",")[-1].strip()
            city = ""
            if len(zipcode.split(" ")) > 1:
                city = zipcode
                zipcode = zipcode.split(" ")[-1]
                if zipcode.isalpha():
                    zipcode = ""
                else:
                    city = city.replace(zipcode,"")
            else:
                city = address.split(",")[-2].strip()
            item_loader.add_value("city", city.strip())
            if zipcode:
                item_loader.add_value("zipcode", zipcode.strip())

        rent = response.xpath("//span[@class='property__title--price']/text()").extract_first()
        if rent:
            if "pw" in rent.lower():
                rent = rent.lower().split('£')[-1].split('p')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))     
                item_loader.add_value("currency", 'GBP')           
            else:
                item_loader.add_value("rent_string", rent.replace(",",""))        

        room_count = response.xpath("//p[@class='property__info']/text()[contains(.,'Bedroom')]").extract_first()
        if room_count:   
            item_loader.add_value("room_count",room_count.split("Bedroom")[0])      
        bathroom_count = response.xpath("//div[@id='panel1']//ul/li[contains(.,'bathrooms') or contains(.,'Bathrooms')]//text()").extract_first()
        if bathroom_count:   
            bathroom_count = bathroom_count.lower().strip("bathrooms")[0].replace("en-suites","").strip().split(" ")[-1].strip()
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count",bathroom_count)          
  
        images = [x for x in response.xpath("//div[@id='gallery-1']/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//div[@id='panel2']/img[@alt='Floorplan']/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        script_map = response.xpath("//script[contains(.,'google.maps.LatLng(')]/text()").get()
        if script_map:
            latlng = script_map.split("google.maps.LatLng(")[1].split(");")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
  
        elevator = response.xpath("//div[@id='panel1']//ul/li[contains(.,' lift')]//text()").extract_first()    
        if elevator:
            item_loader.add_value("elevator", True) 
        terrace = response.xpath("//div[@id='panel1']//ul/li[contains(.,'terrace') or contains(.,'Terrace')]//text()").extract_first()    
        if terrace:
            item_loader.add_value("terrace", True) 
        parking = response.xpath("//div[@id='panel1']//ul/li[contains(.,'Parking') or contains(.,'parking') or contains(.,'Garage')]//text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True) 
   
        swimming_pool = response.xpath("//div[@id='panel1']//ul/li[contains(.,'Swimming Pool')]//text()").extract_first()    
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)   
        desc = " ".join(response.xpath("//div[@id='panel1']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        else:
            desc = " ".join(response.xpath("//div[@id='panel1']//div[contains(@class,'toggle_content')]//text()").extract())
            if desc:
                item_loader.add_value("description", desc.strip())
        item_loader.add_value("landlord_name", "Bargets Estate Agents Limited")
        item_loader.add_value("landlord_phone", "020 7402 9494")
        item_loader.add_value("landlord_email", "enquiries@bargets.co.uk")   

        yield item_loader.load_item()