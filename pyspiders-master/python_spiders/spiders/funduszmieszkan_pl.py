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
import re
class MySpider(Spider):
    name = 'funduszmieszkan_pl'
    execution_type='testing'
    country='poland'
    locale='pl'
    external_source = "Funduszmieszkan_PySpider_poland"
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://funduszmieszkan.pl/wyszukaj-mieszkanie",      
                ],
                "property_type": "apartment"
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.jump,
                            meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def jump(self, response):
        for item in response.xpath("//div[contains(@class,'estiweb-future-offer-box estiweb-future-offer-box-vertical')]"):           
            follow_url = item.xpath(".//a[@class='estiweb-future-offer-box-image']/@href").get()
            yield Request(follow_url, callback=self.get_offer_number, dont_filter=True, meta={"property_type":response.meta.get("property_type")})
    
    def get_offer_number(self, response):
        offer_number = response.xpath("//tbody/tr/td[1]/@id").get()
        if offer_number:
            offer_number = offer_number.split("-")[-1]
            url = f"https://funduszmieszkan.pl/offer/investment-offers-aj-ax?investment_id={offer_number}"
        
        yield Request(url, callback=self.parse, dont_filter=True, meta={"property_type":response.meta.get("property_type")})
    
    def parse(self, response):
        data = json.loads(response.body)
        for item in data:  
            url = item["offerUrl"]
            if url and "lokal" in url:
                return

            yield Request(url, callback=self.parse_details, dont_filter=True, meta={"property_type":response.meta.get("property_type"), "item": item})

    # 2. SCRAPING level 2
    def parse_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        property_type = response.meta.get("property_type")
        item_loader.add_value("property_type", property_type)
        
        item = response.meta.get('item')
        
        external_id = item["id"]
        item_loader.add_value("external_id", str(external_id))
        
        rent = item["price"]
        if rent:
            rent = rent.split(".")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "PLN")
        
        room_count = item["apartment_room_number"]
        if room_count:
            item_loader.add_value("room_count", room_count) 
        bathroom_count = item["apartment_bathroom_number"]
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count) 
        square_meters = item["area_total"]
        if square_meters:
            square_meters = square_meters.split(".")[0]
            item_loader.add_value("square_meters", square_meters)
        
        lat = item["location_latitude"]
        if lat:
            item_loader.add_value("latitude", lat)
        lng = item["location_longitude"]
        if lng:
            item_loader.add_value("longitude", lng)
        available_date = item["available_date"]
        if available_date:
            item_loader.add_value("available_date", available_date)
        
        images = [x for x in item["investmentPicturesUrl"]]
        if images:
            item_loader.add_value("images", images)

        floor_plan_images = [x for x in item["plansUrl"]]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
  
        yield Request(response.url, callback=self.populate_item, dont_filter=True, meta={"item_loader": item_loader})
    
    def populate_item(self, response):

        item_loader = response.meta["item_loader"]
       
        title = response.xpath("(//title/text())[1]").get()
        if title:
            item_loader.add_value("title", title)
        
        address = "".join(response.xpath("normalize-space(//div[contains(@class,'offer-label')][contains(.,'Lokalizacja')]/following-sibling::div/text())").getall())
        if address:
            item_loader.add_value("address", address.lstrip())
            item_loader.add_value("city", address.lstrip().split(" ")[0].strip())

        description = "".join(response.xpath("//div[@class='estiweb-future-offer-description']/text()").getall())
        if description:
            item_loader.add_value("description", description)
        
        
        item_loader.add_value("landlord_name", "Funduszmieszkan")  
        item_loader.add_value("landlord_phone", "+48 22 290 42 69")    
        item_loader.add_value("landlord_email", "cok@funduszmieszkan.pl")
        
        yield item_loader.load_item()