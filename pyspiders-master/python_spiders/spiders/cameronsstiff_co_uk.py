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
    name = 'cameronsstiff_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    custom_settings = {
        "LOG_LEVEL" : "ERROR"
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cameronsstiff.co.uk/search-results-grid/?rent&bedrooms=0&radius=2&price_range=0-0&location=England&geo_id=1&geo_type=country&flat&!let&!let_agreed",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.cameronsstiff.co.uk/search-results-grid/?bedrooms=0&radius=2&price_range=0-0&location=England&geo_id=1&geo_type=country&rent&house&!let&!let_agreed",
                ],
                "property_type" : "house"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    total_page = 0
    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        if response.xpath("//div[contains(@class,'pagination-nav psearch--pagination')]/a[last()-1]/text()").get() and self.total_page == 0:
            self.total_page = int(response.xpath("//div[contains(@class,'pagination-nav psearch--pagination')]/a[last()-1]/text()").get())

        for item in response.xpath("//a[@class='psearch--button']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        if page and page <= self.total_page:
            p_url = response.url.split("&resultpage")[0] + f"&resultpage={page}"
            yield Request(p_url, callback=self.parse, meta={"page":page+1, 'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Cameronsstiff_PySpider_"+ self.country)
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)
            item_loader.add_value("address", title)
            
            zipcode = title.split(",")[-1].strip()
            city = title.split(zipcode)[0].strip().strip(",").split(",")[-1].strip()
            
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//h2[contains(@class,'price')]/text()").get()
        if rent:
            price = False
            if "week" in rent:
                price = rent.strip().split(" ")[0].split("£")[1].replace(",","")
                price = str(int(price)*4)               
            elif "month" in rent:
                price = rent.strip().split(" ")[0].split("£")[1].replace(",","")
            if price:
                item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//p[contains(@class,'bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        square_meters = response.xpath("//p[contains(@class,'sqft')]/text()").get()
        if square_meters:
            sqm = str(int(int(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        
        desc = "".join(response.xpath("//article/ul/../p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1]
            if "Third" in floor:
                item_loader.add_value("floor", "3")
            elif "Second" in floor:
                item_loader.add_value("floor", "2")
                
        available_date = response.xpath("//ul[contains(@class,'features')]/li[contains(.,'Available') or contains(.,'AVAILABLE')]/text()").get()
        if available_date:
            available_date = available_date.lower().split("available")[1].replace("from mid","").strip()
            date2 = False
            date_parsed = dateparser.parse(
                        available_date, date_formats=["%d/%m/%Y"]
                    )
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
                    
        furnished = response.xpath("//li[contains(.,'Fully furnished') or contains(.,'FURNISHED') or contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        parking = response.xpath("//li[contains(.,'parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        floor_plan_images = response.xpath("//div[contains(@id,'floor')]/a/img/@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        lat_lng = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if lat_lng:
            latitude = lat_lng.split("latitude  = '")[1].split("'")[0]
            longitude = lat_lng.split("longitude  = '")[1].split("'")[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        images = [ x for x in response.xpath("//section[contains(@class,'carousel')]//a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "CAMERONS STIFF & Co.")
        
        landlord_phone = response.xpath("//section[contains(@class,'office')]//p/text()[contains(.,'020')]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        
        
        landlord_email = response.xpath("//section[contains(@class,'office')]//p/text()[contains(.,'@')]").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.strip())
        
        yield item_loader.load_item()
