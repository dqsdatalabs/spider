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
    name = 'mandylee_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.mandylee.com.au/renting/properties-for-lease/?property_type%5B%5D=Apartment&property_type%5B%5D=Unit&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.mandylee.com.au/renting/properties-for-lease/?property_type%5B%5D=House&property_type%5B%5D=Townhouse&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.mandylee.com.au/renting/properties-for-lease/?property_type%5B%5D=Studio&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'listing-item')]/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='next page-numbers']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Mandylee_Com_PySpider_australia")   
        
        
        item_loader.add_xpath("title","//div[@class='suburb-address']/text()")
        item_loader.add_xpath("external_id", "//li[label[.='Property ID']]/div/text()")
        item_loader.add_xpath("room_count", "//li[label[.='Bedrooms']]/div/text()")
        item_loader.add_xpath("bathroom_count", "//li[label[.='Bathrooms']]/div/text()")
        square_meters = response.xpath("//li[label[.='Building Size']]/div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[0].strip())
        address = response.xpath("//div[@class='suburb-address']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].strip())
        rent = response.xpath("//div[@class='suburb-price']/text()").get()
        if "per week" in  rent:
            rent=rent.split("$")[-1].split("per week")[0]
            item_loader.add_value("rent", int(float(rent) * 4))
        item_loader.add_value("currency", 'AUD')
         
        parking = response.xpath("//li[label[.='Garage']]/div/text()").get()
        if parking:
            if parking.strip() == "0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        balcony = response.xpath("//div[@class='detail-description']/span/text()[contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        dishwasher = response.xpath("//div[@class='detail-description']/span/text()[contains(.,'-Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        furnished = response.xpath("//h5/text()[contains(.,'furniture')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        available_date = response.xpath("//li[label[.='Available From']]/div/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        description = " ".join(response.xpath("//div[@id='property-description']//div[@class='detail-description']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//div[@class='main-gallery']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//div[@data-class='floorplan']//div[@class='carousel']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        script_map = response.xpath("//script[contains(.,'L.marker([')]/text()").get()
        if script_map:
            latlng = script_map.split("L.marker([")[1].split("]")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
 
        item_loader.add_xpath("landlord_name", "//div[contains(@class,'listing-agent')][1]//a/strong//text()")
        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'listing-agent')][1]//p[contains(@class,'phone')]/a/text()")
        item_loader.add_xpath("landlord_email", "//div[contains(@class,'listing-agent')][1]//p[contains(@class,'email')]/a/text()")
        yield item_loader.load_item()