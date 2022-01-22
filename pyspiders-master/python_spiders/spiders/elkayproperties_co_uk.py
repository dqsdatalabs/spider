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
    name = 'elkayproperties_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.elkayproperties.co.uk/notices?c=44&p=1&p_type=1", "property_type": "apartment"},
            
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
        
        seen = False
        for item in response.xpath("//div[@id='main-content']/div[contains(@class,'feature_property_list')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta.get("base_url", response.url)
            url = base_url.replace("&p=1", f"&p={page}")
            yield Request(url, callback=self.parse, meta={"page": page+1, "base_url":base_url, "property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if "Studio" in response.xpath("//ul[@class='list-info']/li[contains(.,'Property Type')]/text()").get():
            item_loader.add_value("property_type", "studio")
        elif "Studio" in response.xpath("normalize-space(//h1/text())").get():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
            
        item_loader.add_xpath("title", "normalize-space(//h1/text())")
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Elkayproperties_Co_PySpider_united_kingdom")
        ext_id = response.xpath("//li[@class='property-label'][span[.='Reference number']]/text()").extract_first()     
        if ext_id:   
            item_loader.add_value("external_id",ext_id.strip())
        address = response.xpath("//div[@class='estate-explore-location']/text()").extract_first()     
        if address:   
            item_loader.add_value("address",address.strip())
            city = ""
            zipcode = ""
            if len(address.split(","))==2:
                zip_city = address.split(",")[-1].strip() 
                zipcode = zip_city.split(" ")[-1].strip() 
                city = zip_city.split(" ")[0].strip() 
            elif len(address.split(","))==3:
                zipcode = address.split(",")[-1].strip()  
                city = address.split(",")[-2].strip()                
            if city:
                item_loader.add_value("city",city.strip())
            if zipcode:
                item_loader.add_value("zipcode",zipcode.strip())
      
        rent = response.xpath("//div[contains(@class,'notice_price')]/h2//text()").extract_first()
        if rent:
            if "pw" in rent.lower():
                rent_pw = re.findall(r'\d+(?:\.\d+)?', rent.replace(",","."))
                if rent_pw:
                    rent = int(rent_pw[0].replace(".",""))*4
                    rent = "£"+str(rent)
            item_loader.add_value("rent_string", rent.replace(",","."))   
            
        desc = " ".join(response.xpath("//div[contains(@class,'property-description')]/div//text()[normalize-space()]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "unfurnished" in desc.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in desc.lower():
                item_loader.add_value("furnished", True)

        room_count = response.xpath("//li[@class='property-label'][span[.='Bedrooms']]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif not room_count:
            if "Studio" in response.xpath("normalize-space(//h1/text())").get():
                item_loader.add_value("room_count", "1")
        bathroom_count = response.xpath("//li[@class='property-label'][span[.='Bathrooms']]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        available_date = response.xpath("//li[@class='property-label'][span[.='Available from']]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        map_coordinate = response.xpath("//script[@type='text/javascript']/text()[contains(.,'showMap(') and contains(.,'function defaultLocation()')]").extract_first()
        if map_coordinate:
            map_coordinate = map_coordinate.split('function defaultLocation()')[1]
            latitude = map_coordinate.split('showMap(')[1].split(',')[0].strip()
            longitude = map_coordinate.split(',')[1].split(');')[0].strip()
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
        square_meters = response.xpath("//div[contains(@class,'property-description')]//text()[contains(.,'Floor area')]").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].split("sq")[0].strip()
            sqm = str(int(float(square_meters) * 0.09290304))
            item_loader.add_value("square_meters", sqm)
             
        images = [x for x in response.xpath("//div[@id='slider']//li/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)   

        floor_plan_images = [x for x in response.xpath("//ul[@class='floor_image_list']/input/@value").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)  

        item_loader.add_value("landlord_phone", "02076131009")
        item_loader.add_value("landlord_email", "info@elkayproperties.co.uk")
        item_loader.add_value("landlord_name", "Elkay Properties")

        yield item_loader.load_item()