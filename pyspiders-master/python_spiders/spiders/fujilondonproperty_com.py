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
    name = 'fujilondonproperty_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://fujilondonproperty.com/properties?kind=1&dummy=&s=&postcode=&low=0&high=9999999&beds=0&bathroom=0&type=Flat&closest_station=", "property_type": "apartment"},
	        {"url": "https://fujilondonproperty.com/properties?kind=1&dummy=&s=&postcode=&low=0&high=9999999&beds=0&bathroom=0&type=House&closest_station=", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for follow_url in response.xpath("//div[@id='list']/article//h2/a/@href").extract():
            follow_url += "?lang=en"
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

        pagination = response.xpath("//ul[@class='pagination']/li[@class='next']/a/@href").get()
        if pagination:
            yield Request(pagination, callback=self.parse, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_xpath("title", "//h1/address/text()")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Fujilondonproperty_PySpider_united_kingdom")
        address = response.xpath("//h1/address/text()").extract_first()     
        if address:   
            item_loader.add_value("address",address.strip())
            item_loader.add_value("zipcode",address.split(", ")[-1].strip())
            item_loader.add_value("city",address.split(",")[-2].strip())
            
        rent = response.xpath("//div[@class='prop-price']/text()[normalize-space()]").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace(",","."))   
        
        room_count =" ".join(response.xpath("//li[contains(@class,'prop-bedroom')]//text()[normalize-space()]").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.split("bedroom")[0])

        bathroom_count =" ".join(response.xpath("//li[contains(@class,'prop-bathroom')]//text()[normalize-space()]").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("bathroom")[0])
        
        
        prop_type = response.meta.get('property_type')
        

        desc = " ".join(response.xpath("//div[@class='prop-description']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

            if "studio" in desc.lower():
                prop_type = "studio"

            if "Available from" in desc:
                try:
                    available_date = desc.split("Available from")[1].split(".")[0].replace(" of","").strip()
                    date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"], languages=['en'])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
                except:
                    pass
        
        item_loader.add_value("property_type", prop_type)
                        
        parking = response.xpath("//div[@id='prop-features']//li//text()[contains(.,'parking') or contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)    

        furnished = response.xpath("//div[@id='prop-features']//li//text()[contains(.,'Furnished') or contains(.,'furnished')]").get()
        if furnished:
            if "furnished or unfurnished" in furnished.lower():
                item_loader.add_value("furnished", True)
            elif "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[@id='prop-features']//li//text()[contains(.,'Lift') or contains(.,'lift')]").get()
        if elevator:
            if "no lift" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "lift" in elevator.lower():
                item_loader.add_value("elevator", True)
        img=response.xpath("//div[@class='prop-images']/div/@style").extract() 
        if img:
            images=[]
            for x in img:
                img_url = x.split("url('")[1].split("');")[0].strip()
                images.append(img_url)
            if images:
                item_loader.add_value("images",  list(set(images)))
        map_coordinate = response.xpath("//script//text()[contains(.,'google.maps.LatLng')]").extract_first()
        if map_coordinate:
            map_coordinate = map_coordinate.split('LatLng(')[1].split(');')[0]
            latitude = map_coordinate.split(',')[0].strip()
            longitude = map_coordinate.split(',')[1].strip()
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
        item_loader.add_value("landlord_phone", " +44 (0)20 8349 8037")
        item_loader.add_value("landlord_name", "Fuji London Property")
        yield item_loader.load_item()