# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'homewoodhomes_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.homewoodhomes.co.uk/search/1.html?showstc=on&showsold=on&instruction_type=Letting&address_keyword=&minprice=&maxprice=&bedrooms=&property_type=Apartment",
                ],
                "property_type": "apartment"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='resultsThumbnail']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            status = item.xpath("./img[@class='corner']/@alt").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = response.url.replace(f"search/{page-1}", f"search/{page}")
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Homewoodhomes_Co_PySpider_united_kingdom")  
        title = response.xpath("//div[@class='result-address']//text()[normalize-space()]").get()
        if title:
            item_loader.add_value("title", title.strip())    
            item_loader.add_value("address", title.strip())    
            item_loader.add_value("zipcode", title.split(",")[-1].strip())    
            item_loader.add_value("city", title.split(",")[-2].strip())    
        item_loader.add_xpath("room_count","//li[img[@alt='beds']]/text()")
        item_loader.add_xpath("bathroom_count","//li[img[@alt='baths']]/text()")
        item_loader.add_xpath("rent_string","//div[@class='result-price']/text()[normalize-space()]")
   
        description = " ".join(response.xpath("//div[@id='propertyDetails']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [response.urljoin(x) for x in response.xpath("//ul[@id='pikachoose']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//li/a[.='Floorplan']/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
   
        item_loader.add_value("landlord_phone", "020 8313 1393")     
        item_loader.add_value("landlord_email", "enquiries@homewoodhomes.co.uk")
        item_loader.add_value("landlord_name", "Homewood Homes")

        balcony = response.xpath("//ul[@class='features']/li/text()[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)  
        parking = response.xpath("//ul[@class='features']/li/text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True) 
        lat_lng = response.xpath("//script[contains(.,'googlemap')]/text()").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split('"googlemap", "')[-1].split('&q=')[-1].split("-")[0].split("%")[0])
            item_loader.add_value("longitude", lat_lng.split('"googlemap", "')[-1].split('-')[-1].split('"')[0])
        yield item_loader.load_item()