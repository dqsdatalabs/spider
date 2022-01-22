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
class MySpider(Spider):
    name = 'goandco_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.goandco.co.uk/properties-to-rent/flats-to-rent-in-london/page-1",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.goandco.co.uk/properties-to-rent/houses-to-rent-in-london/page-1"
                ],
                "property_type": "house"
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
        for item in response.xpath("//ul[@class='results-list']//div[contains(@class,'image-holder')]"):
            follow_url = response.urljoin(item.xpath(".//@href").get())
            room_count = item.xpath("./../div//li[@class='Bedrooms']/text()[normalize-space()]").get()
            bathroom_count = item.xpath("./../div//li[@class='Bathrooms']/text()[normalize-space()]").get()
            status = item.xpath(".//span[@class='sticker']/text()").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'),"room_count":room_count,"bathroom_count":bathroom_count})
            seen = True
        
        if page == 2 or seen:
            url = response.url.replace(f"page-{page-1}", f"page-{page}")
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("room_count", response.meta.get('room_count'))
        item_loader.add_value("bathroom_count", response.meta.get('bathroom_count'))

        item_loader.add_value("external_source", "Goandco_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//h1/text()")

        rent = response.xpath("//div[@class='section-head-inner']//span[@class='price-qualifier']/text()").extract_first()
        if rent:
            price = rent.replace("£","").strip().replace(",","")
            item_loader.add_value("rent", str(int(float(price))*4))
        item_loader.add_value("currency", "GBP")
        
        item_loader.add_xpath("latitude", "//div[@id='street']/@data-lat")
        item_loader.add_xpath("longitude", "//div[@id='street']/@data-lng")
        address = response.xpath("//h1/text()").get()
        if address:
            address = address.split("rent in")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
             
        features = " ".join(response.xpath("//ul[@class='attributes']/li/text()").getall())
        if features:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq|sqft)",features.replace(",",""))
            if unit_pattern:
                square=unit_pattern[0][0]
                sqm = str(int(float(square) * 0.09290304))
                item_loader.add_value("square_meters", sqm)
        description = " ".join(response.xpath("//div[@class='property-description-inner']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [x for x in response.xpath("//div[@class='owl-carousel']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//div[@id='floorplan']/img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        floor = response.xpath("//li/text()[contains(.,'floor') and not(contains(.,'Underfloor'))]").get()
        if floor:
            item_loader.add_value("floor", floor.split("floor")[0].strip().split(" ")[-1])      
      
        balcony = response.xpath("//li/text()[contains(.,'balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)   
        terrace = response.xpath("//li/text()[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)   
        swimming_pool = response.xpath("//li/text()[contains(.,'swimming pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)   
        parking = response.xpath("//li/text()[contains(.,'parking') or contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True) 

        landlord_name = response.xpath("//div[@id='generic-sidebar']//h2[@class='team-name']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())  
        else:
            item_loader.add_value("landlord_name", "Gordon & Co")
        landlord_phone = response.xpath("//div[@id='generic-sidebar']//span[@class='team-telephone']//a[@class='hidden-xs phone']/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())  
        else:
            item_loader.add_value("landlord_phone", "020 7223 3100")  

        yield item_loader.load_item()