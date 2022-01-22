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
    name = 'rent2dayleiden_nl'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    thousand_separator = ','
    scale_separator = '.'       
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://rent2day.nl/property-listings/?property-search=true&property-id=&s-location=&s-status=186&s-type=480&min-bed=&min-bath=&l-price=0&u-price=2495&l-area=0&u-area=165",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://rent2day.nl/property-listings/?property-search=true&property-id=&s-location=&s-status=186&s-type=631&min-bed=&min-bath=&l-price=0&u-price=2495&l-area=0&u-area=165",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://rent2day.nl/property-listings/?property-search=true&property-id=&s-location=&s-status=186&s-type=666&min-bed=&min-bath=&l-price=0&u-price=2495&l-area=0&u-area=165",
                ],
                "property_type" : "room"
            },
            {
                "url" : [
                    "https://rent2day.nl/property-listings/?property-search=true&property-id=&s-location=&s-status=186&s-type=486&min-bed=&min-bath=&l-price=0&u-price=2495&l-area=0&u-area=165",
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
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='title']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        

        if page == 2 or seen:
            s_type = response.url.split("s-type=")[1].split("&")[0].strip()
            p_url = f"https://rent2day.nl/property-listings/page/{page}/?property-search=true&property-id&s-location&s-status=186&s-type={s_type}&min-bed&min-bath&l-price=0&u-price=2495&l-area=0&u-area=165"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"], "page":page+1}
            )    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Rent2dayleiden_PySpider_netherlands")

        title = response.xpath("//h1[@class='page-title']//text()").extract_first()
        if title:
            item_loader.add_value("title", title.strip())            
      
        address = response.xpath("//h1[@class='page-title']//text()").extract_first()
        if address:
            item_loader.add_value("address",address.replace("|",",").strip() ) 
            item_loader.add_value("city",address.split("|")[-1] .strip() ) 
        
        item_loader.add_xpath("bathroom_count", "//ul/li[contains(.,'Bathroom')]/text()")
        item_loader.add_xpath("external_id", "//ul/li[contains(.,'ID')]/text()")

        room_count =response.xpath("//ul/li[contains(.,'Bedroom')]/text()").extract_first()
        if room_count:                
            item_loader.add_value("room_count",room_count) 
        elif "studio" in response.meta.get('property_type'):
            item_loader.add_value("room_count","1") 
    
        rent ="".join(response.xpath("//div[@class='price']//text()").extract())
        if rent:                
            item_loader.add_value("rent_string",rent)  

        deposit =response.xpath("//ul/li[contains(.,'Deposit')]/span/text()").extract_first()
        if deposit:     
            if "month" in deposit:
                deposit_month = deposit.split("month")[0].strip()
                if "€" in rent:
                    price = rent.split("€")[1].split("/")[0].replace(",","").strip()
                    deposit = int(deposit_month)*int(price)
            item_loader.add_value("deposit", deposit) 

        utilities =response.xpath("//ul/li[contains(.,'Special features')]//text()[contains(.,'service') and contains(.,'€')]").extract_first()    
        if utilities:
            item_loader.add_value("utilities", utilities)
        else:
            utilities =response.xpath("//strong[contains(.,'- included')]/text()").get()    
            if utilities:
                item_loader.add_value("utilities", "".join(filter(str.isnumeric, utilities)))

        square = response.xpath("//ul/li[contains(.,'Area')]/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters",square_meters) 

        available_date =response.xpath("//ul/li[contains(.,'Available')]/span/text()").extract_first()
        if not available_date:
            available_date ="".join(response.xpath("//div[@class='section']/p[contains(.,'Available from the')]//text()").extract())
            available_date = available_date.split("Available from the")[1].split(".")[0]
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().replace("immediately","now"), languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
    
        desc = " ".join(response.xpath("//div[@class='section']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-wrap']//div[@class='item']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        dishwasher =response.xpath("//ul/li[contains(.,' dishwasher')]/@class").extract_first()    
        if dishwasher:
                item_loader.add_value("dishwasher", True)
        else:
            item_loader.add_value("dishwasher", False)

        balcony =response.xpath("//ul/li[contains(.,'Balcony')]/@class").extract_first()    
        if balcony:
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)

        furnished =response.xpath("//ul/li[contains(.,'Furnished')]/@class").extract_first()    
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)

        washing_machine =response.xpath("//ul/li[contains(.,'Washing machine')]/@class").extract_first()    
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        else:
            item_loader.add_value("washing_machine", False)

        item_loader.add_xpath("latitude", "//div[@class='map-wrap']/@data-latitude")
        item_loader.add_xpath("longitude", "//div[@class='map-wrap']/@data-longitude")

        item_loader.add_value("landlord_name", "Rent2day")
        item_loader.add_value("landlord_phone", "+31 6 44 33 38 03")
        item_loader.add_value("landlord_email", "info@rent2day.nl")

        yield item_loader.load_item()