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
    name = 'dynesresidential_com'
    execution_type='testing' 
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'   

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.dynesresidential.com/search/1650/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.dynesresidential.com/search/1651/",
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

        for item in response.xpath("//ul[@id='list']/li/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//li[@class='next']/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Dynesresidential_PySpider_united_kingdom")          
        item_loader.add_xpath("title","//h1/text()")
        item_loader.add_xpath("room_count", "//li[span[.='Bedrooms']]/span[2]/text()")
        item_loader.add_value("external_id", response.url.split("property/")[1].split("/")[1])

        dontallow=response.xpath("//span[contains(.,'Status')]/following-sibling::span/text()").get()
        if dontallow and "agreed" in dontallow.lower():
            return  

        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.replace("   "," ").strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
    
        item_loader.add_xpath("rent_string", "//li[span[.='Price']]/span[2]/text()")
     
        terrace = response.xpath("//li[span[.='Style']]/span[2]//text()[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        energy_label = response.xpath("//li[span[.='EPC Rating']]/span[2]//text()[contains(.,'/')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label[0])
       
        parking = response.xpath("//ul[@class='feats mt20']/li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        floor = response.xpath("//ul[@class='feats mt20']/li[contains(.,'Floor ')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("Floor ")[0].strip().split(" ")[-1].strip())
        description = " ".join(response.xpath("//div[@class='textbp mt20 mb20']/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        script_map = response.xpath("//script[contains(.,'myLatLng = {lat:')]/text()").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("myLatLng = {lat:")[1].split(",")[0].strip())
            item_loader.add_value("longitude", script_map.split("myLatLng = {lat:")[1].split("lng:")[1].split("}")[0].strip())
        images = [response.urljoin(x) for x in response.xpath("//ul[@id='gallery']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
      
        item_loader.add_value("landlord_name", "Dynes Residential")
        item_loader.add_value("landlord_phone", "028 4272 1217")
        item_loader.add_value("landlord_email", "hello@dynesresidential.com")

        yield item_loader.load_item()