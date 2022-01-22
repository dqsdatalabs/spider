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
    name = 'livresidential_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://livresidential.nl/huurwoningen?range%5Bprice%5D=500%3A3500&refinementList%5Btype%5D%5B0%5D=Appartement",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(@class,'flex flex-col')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Livresidential_PySpider_netherlands")
     
        title =" ".join(response.xpath("//div[contains(@class,'lg:justify-between')]//h1//text()").extract())
        if title:
            item_loader.add_value("title",title.strip() )       
            item_loader.add_value("address",title.strip())
        city =response.xpath("//div[contains(@class,'lg:justify-between')]//p[1]//text()").extract_first()
        if city:
            zipcode = " ".join(city.strip().split(" ")[0:2])
            city_value = city.replace(zipcode,"").split("(")[0].strip()
            if not city_value:
                zipcode =city.strip().split(" ")[0]
                city_value = city.replace(zipcode,"").split("(")[0].strip()
            item_loader.add_value("city",city_value.strip())                
            item_loader.add_value("zipcode",zipcode.strip())                
        rent =" ".join(response.xpath("//div[contains(@class,'lg:justify-between')]//p[contains(.,'€')]//text()").extract())
        if rent:     
           item_loader.add_value("rent_string", rent)   
         
        available_date = response.xpath("substring-after(//div[contains(@class,'lg:justify-between')]//p[contains(.,'Beschikbaar vanaf')]//text(),'Beschikbaar vanaf')").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date, languages=['nl'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
      
        floor = response.xpath("//div[div[.='Etage']]/div[2]/text()").extract_first() 
        if floor:   
            item_loader.add_value("floor",floor.strip())  
                
        room_count = response.xpath("//div[contains(.,'slaapkamer')]/text()").re_first(r"\d+")
        if room_count:
            item_loader.add_value("room_count",room_count)
      
        square = "".join(response.xpath("//div[contains(@class,'leading-relaxed')]//div[contains(.,'Area')]//text()").getall())
        if square:
            square_meters = square.split("Area")[1].split("sqm")[0].strip()
            item_loader.add_value("square_meters", square_meters) 

        balcony = "".join(response.xpath("//div[contains(@class,'leading-relaxed')]//div[contains(.,'Balcony')]//text()").getall())       
        if balcony:
            item_loader.add_value("balcony", True) 

        terrace = response.xpath("//div[div[.='Dakterras']]/div[2]/text()").extract_first()       
        if terrace:
            item_loader.add_value("terrace", True) 
        furnished = response.xpath("//div[div[.='Huurcondities']]/div[2]/text()").extract_first()       
        if furnished:
            if "Gestoffeerd" in furnished:
                item_loader.add_value("furnished", True) 
        elevator = "".join(response.xpath("//div[contains(@class,'leading-relaxed')]//div[contains(.,'Elevator')]//text()").getall())       
        if elevator:
            item_loader.add_value("elevator", True) 
        utilities =" ".join(response.xpath("//div[div[contains(.,'Servicekosten')]]/div[2]/text()").extract())       
        if utilities:
            item_loader.add_value("utilities",utilities) 
  
        desc = " ".join(response.xpath("//div[contains(@class,'max-w-2xl')]/div/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
      
        script_map = response.xpath("//script/text()[contains(.,'lat') and contains(.,'lng')]").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("lat =")[1].split(";")[0].strip())
            item_loader.add_value("longitude", script_map.split("lng =")[1].split(";")[0].strip())
        images = [response.urljoin(x)for x in response.xpath("//div[contains(@class,'rounded hover:shadow js-gallery')]/img/@data-src").extract()]
        if images:
                item_loader.add_value("images", images)
            
        item_loader.add_value("landlord_name", "LIV RESIDENTIAL")

        yield item_loader.load_item()