# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from typing import Container
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'parkestateslondon_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = 'Parkestateslondon_Co_PySpider_united_kingdom_en'
    def start_requests(self):
        start_urls = [
            {
                "property_type" : "studio",
                "prop" : "studio"
            },
            {
                "property_type" : "flat",
                "prop": "apartment"
            },
            {
                "property_type" : "maisonette",
                "prop": "house"
            }
        ]
        for url in start_urls:

            formdata = {
                "search_prop": "1",
                "property_type": f"{url.get('property_type')}",
                "bed_min": "",
                "bed_max": "",
                "price_min": "",
                "price_max": "",
                "order_by": "",
                "prop_type": "Lettings",
                "prop_off": "",
            }

            yield FormRequest(
                url="https://parkestateslondon.co.uk/property-code.php",
                callback=self.jump,
                formdata=formdata,
                meta={'prop': url.get('prop')}
            )

    # 1. FOLLOWING
    def jump(self, response):
        
        prop = response.meta.get("prop")
        for item in response.xpath("//div[@class='property-list property']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'prop': response.meta.get('prop')})       
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Parkestateslondon_Co_PySpider_"+ self.country)
        
        prop_type = response.xpath("//span[contains(.,'Type')]/following-sibling::text()").get()
        if prop_type:
            if 'apartment' in prop_type.lower():
                item_loader.add_value("property_type", 'apartment')
            else:
                item_loader.add_value("property_type", response.meta.get('prop'))
        else:
            item_loader.add_value("property_type", response.meta.get('prop'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        title = response.xpath("//div[@class='property-desc']/h2//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        city = response.xpath("(//h2/span/text())[2]").get()
        if city:
            city = city.split(",")[0].strip()
            item_loader.add_value("city", city)
        
        zipcode = response.xpath("//span[@class='post-code-split']//text()").get()
        if zipcode:
            zipcode = zipcode.split("£")[0].strip().split(" ")[-1]
            item_loader.add_value("zipcode", zipcode)

        address = response.xpath("normalize-space(//h2)").get()
        if address:
            item_loader.add_value("address", address)
        
        rent = response.xpath("//h3//text()[contains(.,'pcm')]").get()
        if rent:
            price = rent.replace(",","").strip()
            item_loader.add_value("rent", price)
            
        item_loader.add_value("currency", "GBP")
        room_count = response.xpath("//p[@class='number-tag']//text()[contains(.,'Bedroom')]").get()
        if room_count:           
            item_loader.add_value("room_count", room_count.split(" ")[0])
        else:
            room_count = 1
            item_loader.add_value("room_count", room_count)

        bathroom = response.xpath("//p[@class='number-tag']//text()[contains(.,'Bathroom')]").get()
        if bathroom:    
            item_loader.add_value("bathroom_count", bathroom.split(" ")[0])
        elif bathroom:
            bath = response.xpath("//p[@class='number-tag']//text()[contains(.,'Bathrooms')]").get()
            item_loader.add_value("bathroom_count", bath.split(" ")[0])
        
        desc = "".join(response.xpath("normalize-space(//div[contains(@class,'description-resize')])").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        square_meters = response.xpath("//ul[@class='features-list']/li/text()[contains(.,'sq')]").get()
        if square_meters:
            sq = square_meters.split("sq")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", sq)

        floor = response.xpath("//ul[@class='features-list']/li/text()[contains(.,'Floor')]").get()
        if floor:
            floor = floor.split("floor")[0].strip().split(" ")[0]
            if floor[0].isdigit():
                item_loader.add_value("floor", floor)

        images = [x for x in response.xpath("//div[@class='property-image-sliders']/div//img/@src[contains(.,'images')]").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        item_loader.add_value("landlord_name", "PARK ESTATES")
        item_loader.add_value("landlord_phone", "02077248888")
        item_loader.add_value("landlord_email", "park.estates@btconnect.com")
        
        
        
        
        yield item_loader.load_item()
