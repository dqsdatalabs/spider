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
from  geopy.geocoders import Nominatim
import re

class MySpider(Spider):
    name = 'abacusestates_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Abacusestates_PySpider_united_kingdom_en"
    
    def start_requests(self):

        start_urls = [
            {
                "type" : "House",
                "property_type" : "house"
            },
            {
                "type" : "Flat",
                "property_type" : "apartment"
            },
            {
                "type" : "Apartment",
                "property_type" : "apartment"
            },
            {
                "type" : "Room To Let",
                "property_type" : "room"
            },
            {
                "type" : "Studio",
                "property_type" : "studio"
            },
            
        ] #LEVEL-1

        for url in start_urls:
            r_type = url.get("type")
            payload = {
                "propsearchtype": "",
                "searchurl": "/results??displayorder=propage",
                "market": "1",
                "ccode": "UK",
                "view": "grid",
                "pricetype": "3",
                "pricelow": "",
                "pricehigh": "",
                "propbedr": "",
                "propbedt": "",
                "proptype": r_type,
                "area": "",
                "statustype": "0",
            }

            yield FormRequest(url="https://www.abacusestates.com/results",
                                callback=self.parse,
                                formdata=payload,
                                dont_filter=True,
                                #headers=self.headers,
                                meta={'property_type': url.get('property_type')})
            
    # 1. FOLLOWING
    def parse(self, response): 

        for item in response.xpath("//a[contains(@class,'more-info')]/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )

        next_page = response.xpath("//a[.='Next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")}
            )
        

        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        externalid=response.url
        if externalid:
            item_loader.add_value("external_id",externalid.split("/")[-2])
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        rent=response.xpath("//h2/span/text()").get()
        if "pcm" in rent and "." in rent:
            item_loader.add_value("rent_string", rent.replace(",","").split(".")[0])
        if "pcm" in rent:
            item_loader.add_value("rent_string", rent.replace(",",""))
        elif "." in rent:
            price=rent.split(".")[0].split("£")[1].strip().replace(",","")
            item_loader.add_value("rent_string", str(int(price)*4)+"£")
        elif "request" in rent:
            return
        elif rent:
            price=rent.split("pw")[0].split("£")[1].strip().replace(",","")
            item_loader.add_value("rent_string", str(int(price)*4)+"£")
        
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')][1]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("LatLng(")[1].split(",")[0].strip()
            longitude = latitude_longitude.split("LatLng(")[1].split(",")[1].split(")")[0].strip()
        
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            
        address=response.xpath("//h1/a/span/text()").get()
        if address:
            zipcode = address.split(",")[-1].strip()
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode)
            
        
        desc="".join(response.xpath("//div[contains(@class,'desc')]//p/text()").getall())
        room_count=response.xpath("//div[@class='detail-attributes']/span[contains(.,'Bedroom')]/span/text()").get()
        room = response.xpath("//span[@class='receptions']/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif room:
            item_loader.add_value("room_count", room)
        elif "rooms" in desc.lower():
            room_c=desc.lower().split("rooms")[0].replace("-"," ").strip().split(" ")[-1].strip()
            if room_c.isdigit():
                item_loader.add_value("room_count", room_c)
        
        bathroom_count = response.xpath("//span[@class='bathrooms']/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.replace("\r\n","").strip()))
        if "dishwasher" in desc.lower():
            item_loader.add_value("dishwasher", True)
        if "washing machine" in desc.lower():
            item_loader.add_value("washing_machine", True)
        if ("furnished" in desc.lower()) and ("unfurnished" not in desc.lower()):
            item_loader.add_value("furnished", True)
        if "lift" in desc.lower():
            item_loader.add_value("elevator", True)
        if "balcony" in desc.lower():
            item_loader.add_value("balcony", True)
        if "swimming pool" in desc.lower():
            item_loader.add_value("swimming_pool", True)
        if "sqft" in desc.lower():
            sqmt=desc.lower().split("sqft")[0].strip().split(" ")[-1].replace("(","").replace(",","")
            sqm = str(int(int(sqmt.replace(",",""))* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        elif "sq ft" in desc.lower():
            sqmt=desc.lower().split("sq ft")[0].strip().split(" ")[-1].replace("(","").replace(",","")
            sqm = str(int(int(sqmt)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        
        
        floor_plan_images = response.xpath("//div[@id='detail-floorplan']/img/@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        elevator=response.xpath("//div[@class='prop-bullets']/p[contains(.,'Lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
            
        square_meters=response.xpath("//div[@class='prop-bullets']/p[contains(.,'SQFT')]/text()").get()
        if square_meters:
            sqmt=square_meters.split("SQFT")[0].strip().replace(",","")
            sqm = str(int(int(sqmt)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        
        terrace = response.xpath("//div[@class='prop-bullets']/p[contains(.,'Terrace') or contains(.,'terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        images=[x for x in response.xpath("//ul[@class='slides-container']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        item_loader.add_value("landlord_name","ABACUS")
        item_loader.add_value("landlord_phone","020 3815 5777")
        item_loader.add_value("landlord_email","kensalrise@abacusestates.com")
        
        yield item_loader.load_item()
