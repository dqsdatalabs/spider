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
    name = 'immolissens_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.immolissens.be/nl/te-huur?&ptype=2",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.immolissens.be/nl/te-huur?&ptype=1",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.immolissens.be/nl/te-huur?&ptype=3",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='image']/a"):
            status = item.xpath("./div[@class='marquee']/@style").get()
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        ext_id = response.url.split("&id=")[1].split("&")[0].strip()
        if ext_id:
            item_loader.add_value("external_id", ext_id)
        

        item_loader.add_value("external_source", "Immolissens_PySpider_belgium")
        title =response.xpath("//div/h3[1]/text()").extract_first()
        if title:
            item_loader.add_value("title",title.strip())

        address =response.xpath("//div[div[contains(.,'Adres')]]/div[@class='value']/text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip() )  
            address = address.split(",")[-1].strip()  
            item_loader.add_value("city"," ".join(address.split(" ")[1:]) )  
            item_loader.add_value("zipcode",address.split(" ")[0].strip() )  
 
        item_loader.add_xpath("utilities","//div[div[contains(.,'Lasten huurder')]]/div[@class='value']/text()")      
          
        rent =response.xpath("//div[div[.='Prijs']]/div[@class='value']/text()").extract_first()
        if rent:     
           item_loader.add_value("rent_string", rent)   

        room_count = response.xpath("//div[div[contains(.,'slaapkamer')]]/div[@class='value']/text()").extract_first() 
        if room_count:   
            item_loader.add_value("room_count",room_count.strip())
            
        bathroom_count = response.xpath("//div[div[contains(.,'badkamers')]]/div[@class='value']/text()").extract_first() 
        if bathroom_count:   
            item_loader.add_value("bathroom_count",bathroom_count.strip())

        square =response.xpath("//div[div[contains(.,'Bewoonbare ')]]/div[@class='value']/text()").extract_first()
        if not square:
            square =response.xpath("//div[div[contains(.,'Woongedeelte')]]/div[@class='value']/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 

        desc = " ".join(response.xpath("//div[contains(@class,'prop-html-descr')]//p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        parking = response.xpath("//div[div[contains(.,'Garage')]]/div[@class='value']/text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        available_date = response.xpath("//div[div[contains(.,'Beschikbaarheid')]]/div[@class='value']/text()").extract_first() 
        if available_date:           
            date_parsed = dateparser.parse(available_date, languages=['nl'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
          
        images = [response.urljoin(x) for x in response.xpath("//div[@id='LargePhoto']//a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
        script_map = response.xpath("//script[@type='text/javascript']/text()[contains(.,'MyCustomMarker([')]").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("MyCustomMarker([")[1].split(",")[0].strip())
            item_loader.add_value("longitude", script_map.split("MyCustomMarker([")[1].split(",")[1].split("]")[0].strip())

        item_loader.add_value("landlord_name", "Immo Lissens")
        item_loader.add_value("landlord_phone","013 77 51 63")
        item_loader.add_value("landlord_email","info@immolissens.be")
        elevator =response.xpath("//div[div[contains(.,'Lift')]]/div[@class='value']/text()").extract_first()    
        if elevator:
            if "ja" in elevator.lower():
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
        yield item_loader.load_item()

