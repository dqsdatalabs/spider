# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
class MySpider(Spider):
    name = 'atozpropertyservices_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'     
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://atozpropertyservices.co.uk/properties/page/1/?filter_contract_type=54&filter_location&filter_type=7&filter_bedrooms&filter_bathrooms&filter_price_from&filter_price_to",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://atozpropertyservices.co.uk/properties/page/1/?filter_contract_type=54&filter_location&filter_type=20&filter_bedrooms&filter_bathrooms&filter_price_from&filter_price_to",
                    "https://atozpropertyservices.co.uk/properties/page/1/?filter_contract_type=54&filter_location=&filter_type=216&filter_bedrooms=&filter_bathrooms=&filter_price_from=&filter_price_to=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://atozpropertyservices.co.uk/properties/page/1/?filter_contract_type=54&filter_location=&filter_type=198&filter_bedrooms=&filter_bathrooms=&filter_price_from=&filter_price_to=",
                ],
                "property_type" : "studio"
            },
            {
                "url" : [
                    "https://atozpropertyservices.co.uk/properties/page/1/?filter_contract_type=54&filter_location=&filter_type=209&filter_bedrooms=&filter_bathrooms=&filter_price_from=&filter_price_to=",
                ],
                "property_type" : "room"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@class='properties-grid']/div/div//h2/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        if page == 2 or seen: yield Request(response.url.replace("/page/1/?", f"/page/{page}/?"), callback=self.parse, meta={"page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Atozpropertyservices_Co_PySpider_united_kingdom")
        title = response.xpath("//div/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        address = response.xpath("//div/h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.strip().split(" ")[-1]
            if not zipcode.isalpha():
                zipcode2 = address.strip().split(" ")[-2]
                if not zipcode2.isalpha() and len(zipcode2)<5:
                    zipcode = zipcode2 + " "+zipcode
                item_loader.add_value("zipcode", zipcode)
    
        item_loader.add_xpath("external_id", "//tr[th[.='ID:']]/td/strong/text()")    
        item_loader.add_xpath("city", "//tr[th[.='Location:']]/td//text()")    
            
        rent = "".join(response.xpath("//tr[th[.='Price:']]/td//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))     
    
        room_count = response.xpath("//tr[th[.='Bedrooms:']]/td//text()").get()
        if room_count:        
            item_loader.add_value("room_count",room_count.strip().split("-")[0])
        elif "studio" in response.meta["property_type"]:
            item_loader.add_value("room_count", "1")
        item_loader.add_xpath("bathroom_count","//tr[th[.='Bathrooms:']]/td//text()")
     
        furnished = response.xpath("//tr[th[.='Price:']]/td//text()[contains(.,'furnished') or contains(.,'Furnished')]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        available_date = response.xpath("//div[@class='property-detail']/p//text()[contains(.,'Available')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Available")[1].strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        img = response.xpath("//div[@class='carousel property']/div[@class='content']//li/img/@src").getall()
        if img:
            images=[]
            floor_images=[]
            for x in img:
                if "Floorplan-" in x or "Floor-Plan" in x:
                    floor_images.append(x)
                else:
                    images.append(x)
            if images:
                item_loader.add_value("images",  list(set(images)))
            if floor_images:
                item_loader.add_value("floor_plan_images",  list(set(floor_images)))
       
        desc = " ".join(response.xpath("//div[@class='property-detail']/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())        
        script_map = response.xpath("//script[contains(.,'center     : new google.maps.LatLng(')]/text()").get()
        if script_map:
            latlng = script_map.split("center     : new google.maps.LatLng(")[1].split(")")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip()) 
     
        item_loader.add_value("landlord_name", "A To Z Property Services")
        item_loader.add_value("landlord_phone", "020 8451 8888")    
        item_loader.add_value("landlord_email", "info@atozpropertyservices.co.uk")    
    
        yield item_loader.load_item()