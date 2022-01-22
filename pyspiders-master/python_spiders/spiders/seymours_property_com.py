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


class MySpider(Spider):
    name = 'seymours_property_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    def start_requests(self): 
        start_urls = [
            {
                "url" : [
                    "https://seymours-property.com/search?limit={}&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_division=residential&p_department=RL&propertyAge=&national=false&student=&location=&propertyType=7%2C8%2C9%2C11%2C28%2C29%2C142&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&priceAltered=&orderBy=price%2Bdesc&officeID=&recentlyAdded=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://seymours-property.com/search?limit={}&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_division=residential&p_department=RL&propertyAge=&national=false&student=&location=&propertyType=12%2C14%2C15&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&priceAltered=&orderBy=price%2Bdesc&officeID=&recentlyAdded=",
                    "https://seymours-property.com/search?limit={}&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_division=residential&p_department=RL&propertyAge=&national=false&student=&location=&propertyType=1%2C2%2C3%2C4%2C21%2C22%2C23%2C24%2C26%2C30&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&priceAltered=&orderBy=price%2Bdesc&officeID=&recentlyAdded=",
                    
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(20),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 40)
        seen = False
        for item in response.xpath("//div[@class='main-image']/a"):
            status = item.xpath(".//div[contains(@class,'ribbon base')]/span/text()").get()
            room_count = item.xpath("./../../../../../div[@class='row']//img[contains(@src,'bedroom')]/following-sibling::span/text()").get()
            bathroom_count = item.xpath("./../../../../../div[@class='row']//img[contains(@src,'bathroom')]/following-sibling::span/text()").get()
            if status and "agreed" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"], "room_count":room_count, "bathroom_count":bathroom_count})
            seen = True
        
        if page == 40 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+20, "property_type":response.meta["property_type"], "base_url":base_url})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_id", response.url.split("/")[-1])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="SeymoursProperty_PySpider_united_kingdom_en", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'full_description_large')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[contains(@class,'full_description_large')]//text()[contains(.,'sq feet')]", input_type="F_XPATH", get_num=True, split_list={"sq feet":0, " ":-1}, sq_ft=True)
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value=response.meta["room_count"], input_type="VALUE", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value=response.meta["bathroom_count"], input_type="VALUE", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[@class='price']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='thumbnail_images']//a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div[@id='map']//div[contains(@class,'google-map')]/@data-location", input_type="F_XPATH", split_list={",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div[@id='map']//div[contains(@class,'google-map')]/@data-location", input_type="F_XPATH", split_list={",":1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'Garage') or contains(.,'parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Unfurnished')]", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//strong[contains(.,'Contact Info')]/following-sibling::li/h3/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//strong[contains(.,'Contact Info')]/following-sibling::li//a[contains(@href,'tel')]//text()", input_type="F_XPATH", split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//strong[contains(.,'Contact Info')]/following-sibling::li//a[contains(@href,'mail')]//text()", input_type="F_XPATH")      
        # ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'Available')]//text()", input_type="F_XPATH", split_list={"/":1})      
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//img[contains(@alt,'floorplan')]//@src", input_type="M_XPATH")

        city= response.xpath("//h1/text()").get()
        if city and "," in city:
            city = city.split(",")[1].strip()
            item_loader.add_value("city", city)
        else:
            item_loader.add_value("city", city)

        available_date = response.xpath("//li[contains(.,'Available')]//text()").get()
        if available_date and "from" in available_date:
            available_date = available_date.split("from")[1].strip()
        
        floor = response.xpath("//div[contains(@class,'full_description_small')]//text()[contains(.,'FLOOR')]").get()
        if floor:
            floor = floor.split("FLOOR")[0].strip()
            item_loader.add_value("floor", floor)
        
        yield item_loader.load_item()