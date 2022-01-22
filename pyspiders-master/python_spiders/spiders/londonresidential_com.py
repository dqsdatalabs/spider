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
    name = 'londonresidential_com' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.londonresidential.com/property-lettings/flats-to-rent-in-london",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.londonresidential.com/property-lettings/houses-to-rent-in-london",
                ],
                "property_type" : "house"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    total_page = None
    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[@class='property-image']"):
            status = item.xpath("./a/span/span/text()").get()
            if status and status.lower().strip() == "under offer":
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            p_url = response.url.split("/page")[0] + f"/page-{page}"
            yield Request(p_url, callback=self.parse, meta={"page":page+1, 'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Londonresidential_PySpider_united_kingdom")

        external_id = response.url.split('/')[-1].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(',')[-1].strip())
            item_loader.add_value("city", address.split(',')[-2].strip())
            
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip().replace('\xa0', ''))

        description = " ".join(response.xpath("//div[@id='overview']/p//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        room_count = response.xpath("//div[@class='property-head']//i[contains(@class,'bedroom')]/../text()[2]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//div[@class='property-head']//i[contains(@class,'bathroom')]/../text()[2]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = response.xpath("//div[contains(@class,'main-head')]//span[@class='price-qualifier']/@data-price").get()
        if rent:
            item_loader.add_value("rent", str(int(float(rent)) * 4))
            item_loader.add_value("currency", 'GBP')
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='slider-nav']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='floorplan']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        floor = response.xpath("//li[contains(text(),'Floor')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.lower().split('floor')[0].strip())

        parking = response.xpath("//li[contains(text(),'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//li[contains(text(),'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator = response.xpath("//li[contains(text(),'Lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//li[contains(text(),'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_xpath("landlord_name", "//div[@class='right-contact']/h4/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='right-contact']/p[2]/a[contains(@href,'tel')]/text()")

        yield item_loader.load_item()
