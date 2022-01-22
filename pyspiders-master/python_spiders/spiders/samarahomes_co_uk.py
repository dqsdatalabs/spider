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
from html.parser import HTMLParser 
import dateparser
import re

class MySpider(Spider): 
    name = 'samarahomes_co_uk'
    start_urls = ["https://www.samarahomes.co.uk/student-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=92&minPrice=&maxPrice=&minBedrooms=&maxBedrooms="]
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    
    # 1. FOLLOWING
    def parse(self, response):

        # url = "https://www.samarahomes.co.uk/property/residential/for-rent/west-yorkshire/leeds/clarendon-road-leeds/53207519"
        # yield Request(url, callback=self.populate_item )
        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[@class='card']/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
            )
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.samarahomes.co.uk/student-lettings?page={page}&view=grid&distance=6&propertyTypes[]=92"
            yield Request(
                p_url, 
                callback=self.parse,
                meta={"page":page+1} 
            )
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        prop_type = "".join(response.xpath("//h1/text()").extract())
        if prop_type and ("apartment" in prop_type.lower() or "flat" in prop_type.lower()):
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "house" in prop_type.lower():
            item_loader.add_value("property_type", "house")
        elif prop_type and "student" in prop_type.lower():
            item_loader.add_value("property_type", "student_apartment")
        else: 
            return
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Samarahomes_PySpider_"+ self.country + "_" + self.locale)
        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)

        address = response.xpath("//h2/text()").get()
        if address:
            address = address.strip()
            item_loader.add_value("address", address)
            city=address.split(",")[-1].strip()
            if city:
                city1=re.search("[A-Z][^A-Z]*",city)
                if len(city1.group())>3:
                    item_loader.add_value("city", city1.group())
            item_loader.add_value("zipcode", address.split(",")[-1].split(" ")[2:])

        external_id = "".join(response.xpath("//p/text()[contains(.,'Property reference')]").extract())
        if external_id:  
            external_id  = external_id.split("-")[1].strip()
            item_loader.add_value("external_id", external_id) 

        description ="".join(response.xpath("//div[contains(@class,'property-content')]//p[5]/span/text() | //div[contains(@class,'property-content')]//p[5]/text() | //div[contains(@class,'property-content')]//p[4]/span/text() | //div[contains(@class,'property-content')]//p[3]/text() | //div[contains(@class,'property-content')]//p[4]/text() | //div[contains(@class,'property-content')]//p[2]/strong/text() | //div[contains(@class,'property-content')]//p[2]/text() | //div[contains(@class,'property-content')]//table//p/text() | //div[contains(@class,'property-content')]//p[3]/strong/text() | //div[contains(@class,'property-content')]//p[2]/span/text()").getall())
        if description:
            item_loader.add_value("description", description)
        elif not description:
            desc=response.xpath("//div[contains(@class,'property-content')]//p//strong//text()").getall()
            if desc:
                item_loader.add_value("description", desc)
            

        # room_count = response.xpath("//span[contains(.,'bedroom')]/text()").get()
        # if room_count:
        #     room_count = room_count.strip().replace('\xa0', '').split(' ')[0].strip()
        #     room_count = str(int(float(room_count)))
        #     item_loader.add_value("room_count", room_count)

        rent = response.xpath("//p[@class='property-price']/text()").get()
        if rent:
            rent = rent.split('£')[1].strip().replace('\xa0', '').replace(',', '')

            rent = str(int(rent) * 4)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'GBP')



        images = [x for x in response.xpath("//a[contains(@class,'property-carousel-tile-img')]/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        try:
            available_date ="".join(response.xpath("//span[@class='property-available']//text()").getall())

            if available_date:
                ava_date=available_date.replace("\t","").replace("\n","").split(":")[-1].strip()
                if ava_date:
                    date_parsed = dateparser.parse(ava_date)
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        except:
            pass
 
        item_loader.add_value('landlord_name', 'Samara Lettings')
        item_loader.add_value('landlord_email', 'lettings@samarahomes.co.uk')
        item_loader.add_value('landlord_phone', '0113 244 2443')
      
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data