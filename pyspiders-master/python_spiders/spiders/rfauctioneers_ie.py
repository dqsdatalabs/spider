# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'rfauctioneers_ie'
    execution_type='testing'
    country='ireland'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'    
    start_urls = ["https://rfauctioneers.ie/rentals/"]
    external_source = "Rfauctioneers_PySpider_ireland"
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='elive_property_listings_ad_rest'][div[not(contains(.,'LET AGREED'))]]"):
            follow_url = item.xpath(".//p/a/@href").get()
            prop_type = item.xpath(".//li[1]//text()").get()
            property_type = ""
            if "Apartment" in prop_type:
                property_type = "apartment"
            elif "House" in prop_type:
                property_type = "house"
            elif "Studio" in prop_type:
                property_type = "studio"
            elif "Flat" in prop_type:
                property_type = "apartment"
            if property_type != "":
                yield Request(follow_url, callback=self.populate_item, meta={'property_type' : property_type})
        next_button = response.xpath("//div[contains(@class,'tablenav bottom')]//li[.='Next »']/a/@href").get()
        if next_button: 
            yield Request(next_button, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("&epid=")[-1].split("&")[0])
        item_loader.add_xpath("title","//div[@class='elive_property_addetail_header'][1]/h2//text()")
        address = " ".join(response.xpath("//div[@class='elive_property_addetail_header'][1]/h2//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].replace("Co.","").strip())

        room_count = response.xpath("//div[@class='elive_property_addetail_rest_header']/span[contains(.,'Bedroom')]/text()").get()
        if room_count:                   
            item_loader.add_value("room_count",room_count.split("Bedroom")[0].strip())
        bathroom_count = response.xpath("//div[@class='elive_property_addetail_rest_header']/span[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:                   
            item_loader.add_value("bathroom_count",bathroom_count.split("Bathroom")[0].strip())
        rent = "".join(response.xpath("//span[@class='elive_property_addetail_price']//text()").getall())
        if rent:              
            item_loader.add_value("rent_string", rent) 
    
        description = " ".join(response.xpath("//div[@class='elive_property_addetail_propdescr_text']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

        energy_label = response.xpath("//div[@class='elive_property_addetail_rest_header']/span[contains(.,'BER Rating:')]/strong/text()[.!='Exempt']").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.strip())
        lat_lng = response.xpath("//div[@class='elive_property_addetail_map']/a/@href").get()
        if lat_lng:
            lat_lng = lat_lng.split("/?q=")[1].split("&")[0].strip()
            item_loader.add_value("latitude", lat_lng.split(",")[0].strip())
            item_loader.add_value("longitude", lat_lng.split(",")[1].strip())

        images = [x for x in response.xpath("//div[@class='elive_property_addetail_thumbnails_list_imgcont']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = response.xpath("//div[@class='elive_property_addetail_overview']//li[contains(.,'Furnished') or contains(.,'furnished')]//text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished",False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished",True)
  
        item_loader.add_value("landlord_phone", "061-279423")
        item_loader.add_value("landlord_name", "Rowan Fitzgerald Auctioneers")
        item_loader.add_value("landlord_email", "info@rfauctioneers.ie")
        yield item_loader.load_item()