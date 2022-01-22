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
    name = 'tomfox_ie'
    execution_type='testing'
    country='ireland'
    locale='en'

    def start_requests(self):
        start_url = "https://www.tomfox.ie/property-listings/to-rent/"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(.,'View Property') and following-sibling::div[not(contains(.,'let agreed')) and not(contains(.,'Recently Let'))]]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "".join(response.xpath("//div[@id='description']//text()").getall())
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Tomfox_PySpider_ireland")
        item_loader.add_xpath("title", "//title/text()")

        rent =  "".join(response.xpath("//div[@class='shop-product-heading']/h3/text()").extract())
        if rent:                
            price = rent.split("/")[0]
            item_loader.add_value("rent_string", price)

        address = " ".join(response.xpath("//div[@class='container']/h1/text()").getall())
        if address:
            city = address.split(",")[-1].strip().split(" ")[0]
            zipcode = " ".join(address.split(",")[-1].strip().split(" ")[1:])

            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        item_loader.add_xpath("room_count", "//div[@class='col-md-12']/div[div[.='Bedrooms:']]/div[2]/text()")
        item_loader.add_xpath("bathroom_count", "//div[@class='col-md-12']/div[div[.='Bathrooms:']]/div[2]/text()")
        item_loader.add_xpath("energy_label", "//div[@class='col-md-12']/div[div[.='Energy Rating:']]/div[2]/text()")

        desc =  " ".join(response.xpath("//div[@id='description']//div[@class='col-md-12']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        item_loader.add_xpath("latitude", "substring-before(substring-after(//div[@class='col-md-4']/a/@href,'@'),',')")
        item_loader.add_xpath("longitude", "substring-before(substring-after(substring-after(//div[@class='col-md-4']/a/@href,'@'),','),',')")

        images = [ x for x in response.xpath("//img[@class='ms-thumb']/@src").getall()]
        if images:
            item_loader.add_value("images", images) 

        item_loader.add_value("landlord_name", "TOM FOX ESTATE AGENTS")
        item_loader.add_value("landlord_phone", "(071) 914 4446")
        item_loader.add_value("landlord_email", "INFO@TOMFOX.IE")


        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    else:
        return None