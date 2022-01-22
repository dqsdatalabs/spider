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
    name = 'ghproperty_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    custom_settings = {"HTTPCACHE_ENABLED": False}
    
    thousand_separator = ','
    scale_separator = '.'       

    def start_requests(self):
        yield Request("http://www.ghproperty.com/Lettings", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='feat_property home3']"):
            follow_url = item.xpath("./a/@href").get()
            property_type = item.xpath(".//p/text()").get()
            bedroom = item.xpath(".//li[contains(.,'Beds')]/text()").get()
            bathroom = item.xpath(".//li[contains(.,'Bath')]/text()").get()
            square_meters = item.xpath(".//li[contains(.,'Size:')]/text()").get()
            if get_p_type_string(property_type): 
                yield Request(response.urljoin(follow_url), callback=self.populate_item, meta={"property_type": get_p_type_string(property_type),"bedroom":bedroom,"bathroom":bathroom,"square_meters":square_meters})
        
        next_page = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_page: yield Request(response.urljoin(next_page), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        if response.meta.get('bedroom'):
            item_loader.add_value("room_count", response.meta.get('bedroom').split(":")[-1])
        if response.meta.get('bathroom'):
            item_loader.add_value("bathroom_count", response.meta.get('bathroom').split(":")[-1])
        if response.meta.get('square_meters'):
            item_loader.add_value("square_meters", response.meta.get('square_meters').split(":")[-1])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Ghproperty_PySpider_united_kingdom")
        item_loader.add_xpath("rent_string", "//h2/text()")
        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_value("external_id", response.url.split("/")[-1])
        description=" ".join(response.xpath("//div[@class='listing_single_description']/p//text()").extract())
        if description:
            item_loader.add_value('description',description.strip())
        furnished=response.xpath('//a[contains(.,"Furnished") or contains(.,"furnished")]/text()').extract_first()
        if furnished:
            if "unfurnished" in furnished.lower().strip() or "un-furnished" in furnished.lower().strip():
                item_loader.add_value('furnished',False)
            else:
                item_loader.add_value('furnished',True)
        images = [x for x in response.xpath("//div[@class='listing_single_property_slider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)   
        item_loader.add_value("landlord_name", "Gordon Hughes Estate Agents")
        item_loader.add_value("landlord_phone", "01 8027000")
        item_loader.add_value("landlord_email", "ratoath@ghproperty.com")

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address",address)
            zipcode = address.strip().replace(".","").replace(",","").split(" ")
            if not zipcode[-2].isalpha() and not zipcode[-2].isalpha():
                zipcode = f"{zipcode[-2]} {zipcode[-1]}"
                item_loader.add_value("zipcode", zipcode)
                city = address.split("Co")[1].split(zipcode)[0].replace(",","").strip()
                item_loader.add_value("city", city)
            else:
                city = address.split(",")[-1].strip()
                if " Co" in city:
                    item_loader.add_value("city", address.split(" Co")[1].replace(".","").strip())
                else:
                    item_loader.add_value("city",city.replace(".","").strip())
                
     
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None