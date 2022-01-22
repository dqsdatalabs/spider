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
from datetime import datetime
from word2number import w2n 
import dateparser

class MySpider(Spider):
    name = 'agenthouse_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://agenthouse.co.uk/prime-london/for-rent/"]

    # 1. FOLLOWING
    def parse(self, response):  

        
        for item in response.xpath("//li//div[@class='property-item primary-tooltips title-above-image']"):
            follow_url = response.urljoin(item.xpath("./a/@href").extract_first())
            price = item.xpath("./div//div[@class='price-tag']/text()").extract_first()
            yield Request(follow_url, callback=self.populate_item,meta={"price" : price})
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)

        desc = " ".join(response.xpath("//section[@id='property-content']/p/text()").getall())
        
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            return

        item_loader.add_value("external_source", "Agenthouse_Co_PySpider_united_kingdom")


        address = response.xpath("//h1/span/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        zipcode=response.xpath("//section[@id='location']/p/text()").get()
        if zipcode:
            a=zipcode.split()[-2]
            b=zipcode.split()[-1]
            zipcode=a+" "+b 
            item_loader.add_value("zipcode", zipcode)



            # zipcode = address.split(',')[-1].strip()
            # if zipcode.replace(" ","").isalpha():
            #     item_loader.add_value("city", address.split(',')[-1].strip())
            # else:
            #     item_loader.add_value("zipcode", zipcode)
            #     try: item_loader.add_value("city", address.split(',')[-2].strip())
            #     except: pass
        
        title = response.xpath("//h1/span/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//section[@id='property-content']/p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
            if "Available from the" in description:
                available_date = description.split("Available from the")[1].split(".")[0]
                date_parsed = dateparser.parse(available_date, languages=['en'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        square_meters = response.xpath("//div[@class='property-meta primary-tooltips']//div[contains(text(),'sq ft')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", str(int(int(square_meters.split('sq ft')[0].strip()) * 0.09290304)))

        room_count = response.xpath("//div[@class='property-meta primary-tooltips']//div[contains(text(),'Bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.lower().split('bedroom')[0].strip())
        
        bathroom_count = response.xpath("//div[@class='property-meta primary-tooltips']//div[contains(text(),'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.lower().split('bathroom')[0].strip())

        rent = response.meta.get("price")
       
        if rent:
            if "Price Upon Request" not in rent:
                
                price = rent.split("£")[1].replace("PCM","").replace("pm","").replace(",","")
                item_loader.add_value("rent", str(int(float(price))))
            item_loader.add_value("currency", 'GBP')
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-carousel']/div/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[contains(@id,'floor-plan')]/img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('LatLng(')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip())
        
        parking = response.xpath("//div[@class='property-meta primary-tooltips']//div[contains(text(),'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        item_loader.add_value("landlord_name", "Agent House")
        item_loader.add_value("landlord_phone", "+44 (0)20 7183 4818")
        item_loader.add_value("landlord_email", "home@agenthouse.co.uk")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    else:
        return None
