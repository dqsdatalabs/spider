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
    name = 'warwickestateagents_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.warwickestateagents.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice="}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,)

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'searchItem')]/figure/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.warwickestateagents.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=&page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Warwickestateagents_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        desc="".join(response.xpath("//article/h2/parent::article//text()").getall())
        description=desc.split("Key Features:")[0].strip()
        if "apartment" in description or "flat" in description:
            item_loader.add_value("property_type","apartment")
        elif "house" in description or "maison" in description or "home" in description:
            item_loader.add_value("property_type", "house")
        else: return
        
        item_loader.add_value("description", re.sub('\s{2,}', ' ', description))
        
        rent="".join(response.xpath("//div[@class='fdPrice']/div/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(",",""))
            
        room_count=response.xpath("//div[@class='fdRooms']/span[contains(.,'bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        bathroom_count=response.xpath("//div[@class='fdRooms']/span[contains(.,'bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])

        address = response.xpath("//h3/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2])
            item_loader.add_value("zipcode", address.split(",")[-1])
            
        items=description.split("sq")[0].strip().split(" ")[-1]
        if items:
            try:
                sqm = str(int(int(items)* 0.09290304))
            except ValueError:
                sqm=None
            if sqm:
                item_loader.add_value("square_meters", sqm )
        
        images=[x for x in response.xpath("//div[contains(@class,'royalSlider')]/a/@href").getall()]    
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        floor_plan_images = response.xpath("//a[contains(@href,'floorplan')]//@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        item_loader.add_value("landlord_name","Warwick Estate Agents")
        item_loader.add_value("landlord_email"," info@warwickestateagency.co.uk")
        item_loader.add_value("landlord_phone","020 8960 9988")

        
        yield item_loader.load_item()