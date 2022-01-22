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
from datetime import datetime

class MySpider(Spider):
    name = 'livingsoho_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):

        start_urls = [
            {
                "type" : 37,
                "property_type" : "apartment"
            },
            {
                "type" : 15,
                "property_type" : "house"
            },
            {
                "type" : 19,
                "property_type" : "apartment"
            },
            {
                "type" : 38,
                "property_type" : "apartment"
            },
            {
                "type" : 14,
                "property_type" : "house"
            },
            {
                "type" : 20,
                "property_type" : "studio"
            },
            {
                "type" : 12,
                "property_type" : "house"
            },
            {
                "type" : 33,
                "property_type" : "house"
            },
            
        ] #LEVEL-1

        for url in start_urls:
            r_type = str(url.get("type"))
            payload = {
                "c": "2",
                "l": "",
                "t": r_type,
                "pmin": "",
                "pmax": "",
                "bedrooms": "0",
                "bathrooms": "",
                "o": "1",
            }

            yield FormRequest(url="http://livingsoho.co.uk/search/json",
                                callback=self.parse,
                                formdata=payload,
                                dont_filter=True,
                                #headers=self.headers,
                                meta={'property_type': url.get('property_type')})
            
    # 1. FOLLOWING
    def parse(self, response):

        data = json.loads(response.body)
        sel = Selector(text=data["html"], type="html")
        
        for item in sel.xpath("//a[@class='property-link']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        
        next_page = sel.xpath("//a[.='»']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")}
            )
        
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
    
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_value("external_source", "Livingsoho_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_xpath("title","//div/h1//text()")
        
        address = response.xpath("//title//text()").get()
        if address:
            address = address.replace(".",",")
            item_loader.add_value("address", address)
            count = address.count(",")
            if count == 0:
                item_loader.add_value("city", address)
            elif count == 1:
                item_loader.add_value("city", address.split(",")[-1].strip())
            else:
                item_loader.add_value("city", address.split(",")[-2].strip())
        
        zipcode = response.xpath("//div/@data-location").get()
        if zipcode:
            zipcode = zipcode.split(",")[-1].strip()
            item_loader.add_value("zipcode", zipcode)
        
        desc = " ".join(response.xpath("//div[@class='property-info-detail-container']//text()").extract())
        
        per_week = False
        if "per week" in desc.lower() and "per week/" not in desc.lower():
            per_week = True
        
        price = response.xpath("//div[@class='property-info-container']//li[span[contains(.,'Price')]]/text()").extract_first()
        if price:
            if per_week:
                price = price.replace(" ","").replace("£","").replace(",","").strip()
                price = int(float(price))*4
                
                item_loader.add_value("rent_string", str(price)+"£")
            else:
                item_loader.add_value("rent_string", price.replace(" ","").replace(",",""))
                
        room = response.xpath("//div[@class='property-info-container']//li[span[contains(.,'Bedroom')]]/text()[.!=' 0']").extract_first()
        if room:
            item_loader.add_value("room_count", room.strip())
        elif response.meta.get('property_type') == "studio":
            item_loader.add_value("room_count","1")
        elif "double bedroom" in desc.lower():
            room_count=desc.split("double")[0].strip().split(" ")[-1]
            item_loader.add_value("room_count", room_count)
        elif "bedroom apartment *" in desc.lower():
            room_count=desc.split("bedroom apartment *")[0].strip().split(" ")[-1]
            item_loader.add_value("room_count", room_count)
        elif "bedroom house" in desc.lower():
            room_count = desc.lower().split("bedroom house")[0].strip().split(" ")[-1]
            item_loader.add_value("room_count", room_count)
            
        available_date = response.xpath("//div[@class='property-info-detail-container']//text()[contains(.,'Available')]").get()
        if available_date:
            available_date = available_date.replace("*","").replace(",","")
            match = re.search(r'(\d+/\d+/\d+)', available_date)
            if match:
                newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
                item_loader.add_value("available_date", newformat)
            elif "now" in available_date.lower() or "immediately" in available_date.lower():
                available_date = datetime.now()
                item_loader.add_value("available_date", available_date.strftime("%Y-%d-%d"))
            elif "From" in available_date:
                available_date = available_date.split("From")[1].split(".")[0].replace("-","").strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            elif "from" in available_date:
                available_date = available_date.split("from")[1].replace("Property","-").strip()
                if "-" in available_date:
                    available_date = available_date.split("-")[0].strip()
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                else:
                    available_date = available_date.split("-")[0].strip()
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
        elif "Available from" in desc:
            match = re.search(r'(\d+/\d+/\d+)',desc.replace("*",""))
            if match:
                newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
                item_loader.add_value("available_date", newformat)
            else:
                print(desc.split("Available from")[1].strip())
                    
                    
        if desc:
            item_loader.add_value("description", desc.strip())
            if "no pets" in desc.lower():
                item_loader.add_value("pets_allowed", False)
            if "parking" in desc:
                item_loader.add_value("parking", True)
            if "furnished" in desc:
                item_loader.add_value("furnished", True)
            if "lift" in desc:
                item_loader.add_value("elevator", True)
            if "balcony" in desc:
                item_loader.add_value("balcony", True)
            
                
        images = [x for x in response.xpath("//div[@class='hero-media-item']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "01792 515814")
        item_loader.add_value("landlord_email", "liveinsoholets@gmail.com")
        item_loader.add_value("landlord_name", "Living Soho")
        
        status = response.xpath("//li[contains(.,'Status')]/text()").get()
        if status and "available" in status.lower():
            yield item_loader.load_item()
