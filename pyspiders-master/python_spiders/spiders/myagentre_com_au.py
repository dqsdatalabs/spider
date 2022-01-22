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
    name = 'myagentre_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.myagentre.com.au/renting/properties-for-lease/?property_type%5B%5D=Apartment&property_type%5B%5D=Unit&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.myagentre.com.au/renting/properties-for-lease/?property_type%5B%5D=House&property_type%5B%5D=Townhouse&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "house"
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'container')]/a"):
            status = item.xpath(".//div[@class='sticker']/text()").get()
            if status and "leased" in status.lower():
                continue
            follow_url = item.xpath("./@href").get()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={
                    "property_type":response.meta["property_type"],
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Myagentre_Com_PySpider_australia")  
        item_loader.add_xpath("title", "//div[contains(@class,'address-title')]/h4/text()")        
        address = response.xpath("//div[contains(@class,'address-title')]/h4/text()").get()
        if address:
            item_loader.add_value("address", address.strip())      
        city = response.xpath("//title/text()").get()
        if city:
            item_loader.add_value("city", city.split("|")[0].split(",")[-1].strip())     
        external_id = response.xpath("//p[contains(@class,'property-id')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())    
        item_loader.add_xpath("room_count", "//li[i[contains(@class,'la-bed')]]/span/text()")
        item_loader.add_xpath("bathroom_count", "//li[i[contains(@class,'la-bath')]]/span/text()")
      
        rent = response.xpath("//div[@class='price']/text()").get()
        if rent:
            rent = rent.split("$")[-1].lower().split("p")[0].split("week")[0].strip().replace(",","")
            item_loader.add_value("rent", int(float(rent)) * 4)
        deposit = response.xpath("//li[label[.='Bond Amount']]/div/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(",",""))
        item_loader.add_value("currency", 'AUD')
 
        parking = response.xpath("//li[i[contains(@class,'la-car')]]/span/text()").get()
        if parking:
            item_loader.add_value("parking", True) if parking.strip() != "0" else item_loader.add_value("parking", False)
        balcony = response.xpath("//div[@class='detail-description']//text()[contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        furnished = response.xpath("//h5[@class='sub-title']//text()[contains(.,'furnished') or contains(.,'Furnished') or contains(.,'FURNISHED')]").get()
        if furnished:
            if "UNFURNISHED" in furnished.upper():
                item_loader.add_value("furnished", False)
            elif "FURNISHED" in furnished.upper():
                item_loader.add_value("furnished", True)  
        available_date = response.xpath("//li[label[.='Available From']]/div/text()[.!='Now']").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("AVAILABLE")[-1].strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        script_map = response.xpath("//script[contains(.,'L.marker([')]/text()").get()
        if script_map:
            latlng = script_map.split("L.marker([")[1].split("]")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())

        description = " ".join(response.xpath("//div[@class='detail-description']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        images = [x for x in response.xpath("//div[@class='main-carousel']//div[contains(@class,'slider-image')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
             
        floor_plan_images = [x for x in response.xpath("//div[@class='main-carousel']//div[contains(@class,'slider-floorplan')]//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        item_loader.add_xpath("landlord_name", "//div[contains(@class,'agent-detail')]/a//p/text()")
        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'agent-detail')]/p[contains(@class,'phone')]/a/text()")
        item_loader.add_xpath("landlord_email", "//div[contains(@class,'agent-detail')]/p[contains(@class,'email')]/a/text()")

        yield item_loader.load_item()