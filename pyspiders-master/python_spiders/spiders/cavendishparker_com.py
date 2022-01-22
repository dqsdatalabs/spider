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
    name = 'cavendishparker_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "type" : "Apartment",
                "property_type" : "apartment"
            },
            {
                "type" : "Flat",
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:
            formdata = {
                "TypeToRent": url.get("type"),
                "MinBed": "",
                "MinBath": "",
                "PriceFromRent": "",
                "PriceToRent": "",
            }
            yield FormRequest(
                url="http://cavendishparker.com/lettings.php",
                callback=self.parse,
                formdata=formdata,
                meta={'property_type': url.get('property_type')}
            )

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='price']/preceding-sibling::*/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("unid=")[-1])
        item_loader.add_value("external_source","Cavendishparker_PySpider_"+ self.country)
        item_loader.add_xpath("title", "//div/h1[@class='page-header']//text()")
       
        address = response.xpath("//div/h1[@class='page-header']//text()").extract_first()     
        if address:   
            item_loader.add_value("address",address.strip())
            zipcode = address.split(",")[-1].strip()  
            city = address.split(",")[-2].strip()  
            item_loader.add_value("city",city.strip())
            item_loader.add_value("zipcode",zipcode.replace(".","").strip())
  
        rent = response.xpath("//div[@class='column']/h2[contains(.,'£')]//text()").extract_first()
        if rent:
            if "pw" in rent.lower():
                rent_pw = re.findall(r'\d+(?:\.\d+)?', rent.replace(",","."))
                if rent_pw:
                    rent = int(rent_pw[0].replace(".",""))*4
                    rent = "£"+str(rent)
            item_loader.add_value("rent_string", rent.replace(",","."))   
            
        desc = " ".join(response.xpath("//div[@class='property-detail']/text()[normalize-space()]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
 
        room_count = response.xpath("//ul[@class='unstyled']/li[contains(.,'Bedrooms')]/following-sibling::li[1]//text()").get()
        if room_count and room_count.strip() !="0":
            item_loader.add_value("room_count", room_count)
        elif desc and "STUDIO" in desc.upper():
            item_loader.add_value("room_count", "1")
        bathroom_count = response.xpath("//ul[@class='unstyled']/li[contains(.,'Bathrooms')]/following-sibling::li[1]//text()").get()
        if bathroom_count and bathroom_count.strip() !="0":
            item_loader.add_value("bathroom_count", bathroom_count)

        available_date = response.xpath("//ul//li[contains(.,'AVAILABLE')]//text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("AVAILABLE")[1].strip(), languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        map_coordinate = response.xpath("//script[@type='text/javascript']/text()[contains(.,'google.maps.LatLng(')]").extract_first()
        if map_coordinate:
            map_coordinate = map_coordinate.split('google.maps.LatLng(')[1].split(');')[0]
            latitude = map_coordinate.split(',')[0].strip()
            longitude = map_coordinate.split(',')[1].strip()
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
              
        images = [x for x in response.xpath("//div[@class='ws_images']//li/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)   

        balcony = response.xpath("//ul//li[contains(.,'Balcony') or contains(.,'BALCONY')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True) 

        washing_machine = response.xpath("//ul//li[contains(.,'Washer Dryer') or contains(.,'Washing Machine')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True) 

        dishwasher = response.xpath("//ul//li[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True) 

        parking = response.xpath("//ul//li[contains(.,'Parking') or contains(.,'PARKING')]//text()").get()
        if parking:
            item_loader.add_value("parking", True) 

        furnished = response.xpath("//ul//li[contains(.,'Furnished') or contains(.,'FURNISHED') or contains(.,'furnished')]//text()").get()
        if furnished:
            if "FURNISHED/UNFURNISHED" in furnished.upper():
                pass
            elif "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False) 
            else:
                item_loader.add_value("furnished", True) 
                
        terrace = response.xpath("//ul//li[contains(.,'Terrace') or contains(.,'TERRACE')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True) 
  
        item_loader.add_value("landlord_phone", "020 7846 0846")
        item_loader.add_value("landlord_email", "info@cavendishparker.com")
        item_loader.add_value("landlord_name", "Cavendish Parker")
        
        yield item_loader.load_item()
