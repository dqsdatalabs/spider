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
from datetime import datetime

class MySpider(Spider):
    name = 'charringtons_london'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source="Charringtons_PySpider_united_kingdom"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.charringtons.london/search.ljson?channel=lettings&fragment=tag-apartment",
                    "https://www.charringtons.london/search.ljson?channel=lettings&fragment=tag-flat",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.charringtons.london/search.ljson?channel=lettings&fragment=tag-detached",
                    "https://www.charringtons.london/search.ljson?channel=lettings&fragment=tag-house",
                     
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        data = json.loads(response.body)
        if data:
            for item in data["properties"]:
                follow_url = response.urljoin(item["property_url"])
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"], "items": item})
            
            if data["pagination"]["has_next_page"]:
                base_url = response.meta["base_url"]
                p_url = base_url + f"/page-{page}"
                yield Request(
                    p_url,
                    callback=self.parse,
                    meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)

        let=response.xpath("//h1[@class='heading__title']//span/text()").get()
        if let and "let" in let.lower():
            return 

        ext_id = response.url.split("properties/")[1].split("/")[0].strip()
        if ext_id:
            item_loader.add_value("external_id", ext_id)
        
        title = response.xpath("//h1/text()").get()
        if title:
            prop_type = ""
            if "studio" in title.lower():
                prop_type = "studio"
            elif "apartment" in title.lower():
                prop_type = "apartment"
            else:
                prop_type = response.meta.get('property_type')
            item_loader.add_value("property_type", prop_type)
               
            if "short let" in title.lower():
                return
            item_loader.add_value("title", title.replace("|", "").strip())
            
            address = ""
            if title.count("|") == 3:
                address = "".join(title.split("|")[-2:]).lower().replace("|", "").replace("to let", "").replace("apartment","")
            elif title.count("|") >= 4:
                address = "".join(title.split("|")[-3:]).lower().replace("|", "").replace("to let", "").replace("apartment","")
            if address:
                item_loader.add_value('address', address.strip().upper())
            
            city = title.split("|")[-2].strip()
            if city:
                item_loader.add_value('city', city)

            zipcode = title.split("|")[-1].strip().split()[-1]
            if zipcode and any(z.isdigit for z in zipcode):
                item_loader.add_value("zipcode", zipcode)
            
                
        
        
        items = response.meta.get('items')

        room_count = items["bedrooms"]
        if room_count and room_count != "0":
            item_loader.add_value("room_count", room_count)
        elif "studio" in prop_type:
            item_loader.add_value("room_count", "1")
        
        rent = items["price"]
        if rent:
            if "pw" in rent:
                price = rent.split("pw")[0].replace("£", "").replace(" ","")
                price = str(int(price)*4)
                item_loader.add_value("rent_string", price+"£")
            else:
                price = rent.split(" ")[0]
                item_loader.add_value("rent_string", price)
        lat_lng = response.xpath("//script[contains(.,'LatLng')]/text()").re_first(r"LatLng\((\d+.\d+, -*\d+.\d+)\)")    
        if lat_lng:
             item_loader.add_value("latitude", lat_lng.split(",")[0].strip())
             item_loader.add_value("longitude", lat_lng.split(",")[1].strip())
       
        desc = " ".join(response.xpath("//div[@class='property--content']//p/text()").getall())
        if desc:
            desc = re.sub(r'\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        square_meters = ""
        if "sqft" in desc.lower():
            square_meters = desc.lower().split("sqft")[0].strip().split(" ")[-1].replace(",","")
        elif "sq ft" in desc.lower():
            square_meters = desc.lower().split("sq ft")[0].strip().split(" ")[-1].replace(",","")
            
        if square_meters:
            if "/" in square_meters:
                square_meters = square_meters.split("/")[1]
            sqm = str(int(int(square_meters.replace("(",""))* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        else:
            square_meters = response.xpath("//ul[@class='property-features']/li[contains(.,'sqft') or contains(.,'sq ft')]/text()").get()
            if square_meters:
                if "sqm" in square_meters:
                    square_meters = square_meters.split("sqm")[0].replace("-","").strip()
                else:
                    square_meters = square_meters.split("sq")[0]
                    square_meters = str(int(int(square_meters.replace("(",""))* 0.09290304))
                item_loader.add_value("square_meters", int(float(square_meters)))
        bathroom_count = response.xpath("//h2[contains(.,'bath')]/text()").re_first(r"(\d)\sbath")     
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)   
                                    
        images = [response.urljoin(x) for x in response.xpath("//div[@class='rsContent']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor = response.xpath("//ul[@class='property-features']/li[contains(.,'floor') or contains(.,'Floor')]/text()").get()
        if floor:
            floor = floor.lower().split("floor")[0].strip().split(" ")[-1]
            status = True
            not_list = ["wood", "laminate", "tile", "all"]
            for i in not_list:
                if i in floor:
                    status = False
            if status:
                if "second" in floor:
                    floor = "second"
                item_loader.add_value("floor", floor.capitalize())
        
        if "available now" in desc.lower() or "available immediately" in desc.lower() or "Available: NOW" in desc:
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        elif "available to rent immediately" in desc.lower():
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        elif "Available" in desc:
            available_date = desc.split("Available")[-1].strip().replace(".","")
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            else:
                available_date = available_date.replace("and", ",").replace("Working",",").replace("Wooden",",").replace("from","").replace("Offered",",")
                if "," in available_date:
                    date_parsed = dateparser.parse(available_date.split(",")[0].strip(), date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
    
        elevator = response.xpath("//ul[@class='property-features']/li[contains(.,'Lift') or contains(.,'lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        furnished = response.xpath("//ul[@class='property-features']/li[contains(.,' furnished') or contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        balcony = response.xpath("//ul[@class='property-features']/li[contains(.,'balcon') or contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//ul[@class='property-features']/li[contains(.,'parking') or contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//ul[@class='property-features']/li[contains(.,'terrace') or contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        swimming_pool = response.xpath("//ul[@class='property-features']/li[contains(.,'pool') or contains(.,'Pool')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        floor_plan_images = response.xpath("//a[contains(@class,'floorplan')]/@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        item_loader.add_value("landlord_name", "CHARRINGTONS PROPERTY CONSULTANTS")
        item_loader.add_value("landlord_phone", "020 7112 4852")
        
        
        yield item_loader.load_item()