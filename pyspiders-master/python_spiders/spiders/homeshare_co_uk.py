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
    name = 'homeshare_co_uk'  
    execution_type='testing'
    country='united_kingdom'
    locale='en'  
    custom_settings = {
        "CONCURRENT_REQUESTS" : 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 0.2,
        "PROXY_TR_ON" : True,
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://homeshare.co.uk/rooms/",
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
        for item in response.xpath("//div[@class='property-item']/div/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://homeshare.co.uk/rooms/page/{page}/?wre-orderby"
            yield Request(p_url, callback=self.parse, meta={'page': page+1,'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Homeshare_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("substring-before(//title/text(),' -')").extract_first()     
        if title:   
            item_loader.add_value("title",title.strip())    

        rent = response.xpath("//p[@class='property--price']/text()").extract_first()
        if rent:
            if "pw" in rent:
                numbers = re.findall(r'\d+(?:\.\d+)?', rent.replace(",","."))
                if numbers:
                    rent = int(numbers[0].replace(".",""))*4
                    rent = "£"+str(rent)
            item_loader.add_value("rent_string", rent)
        room = response.xpath("//div[@class='feature--content']/p[contains(.,'Bed')]/text()[not(contains(.,'0 Bed'))]").extract_first()
        if room:
            item_loader.add_value("room_count", room.split("Bed")[0].strip())
        elif not room:
            if "room" in response.meta.get('property_type') or "studio" in response.meta.get('property_type'):
                item_loader.add_value("room_count", "1")    

        bathroom = response.xpath("//div[@class='feature--content']/p[contains(.,'Bath')]/text()").extract_first()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.split("Bath")[0].strip())

        parking = response.xpath("//div[@class='feature--content'][contains(.,'Parking')]/p/text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        dishwasher = response.xpath("//div[@class='feature-item']/p[contains(.,'Dish Washer')]/text()").extract_first()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        city = response.xpath("//ul/li[span[contains(.,'City')]]/text()").extract_first()
        if city:
            item_loader.add_value("city", city.strip())
        zipcode = response.xpath("//ul/li[span[contains(.,'Post code')]]/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        address = response.xpath("//p[@class='property--location']/text()").extract_first()
        if address:   
            item_loader.add_value("address", address.strip())
         
        available_date = response.xpath("//span[@class='property--status']/span[@class='value']/text()").extract_first()
        if available_date:   
            newformat = dateparser.parse(available_date.strip()).strftime("%Y-%m-%d")
            item_loader.add_value("available_date", newformat)
        desc = " ".join(response.xpath("//div[@class='property--details']/p/text()").extract())
        if desc:
            #desc = re.sub("\s{2,}", " ", desc)
            item_loader.add_value("description", desc.strip())
            if "furnished" in desc.lower() and "unfurnished" not in desc.lower():
                item_loader.add_value("furnished", True)
            if "No Pets" in desc:
                item_loader.add_value("pets_allowed", False)
               
        images = [x for x in response.xpath("//ul[@id='image-gallery']/li/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)      

        item_loader.add_xpath("landlord_phone", "//div[@class='agent--contact']//li[i[contains(@class,'fa-phone')]]/a/text()")
        item_loader.add_value("landlord_email", "info@homeshare.co.uk")
        item_loader.add_xpath("landlord_name", "//div[@class='agent--info']/h5/text()")

        yield item_loader.load_item()
