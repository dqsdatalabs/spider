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
    name = 'lisney_com' 
    execution_type='testing' 
    country='ireland'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'       

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://lisney.com/property/residential/to-let/?filter_type=867",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://lisney.com/property/residential/to-let/?filter_type=862%7C873",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url["url"]:
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url['property_type']})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='property_listing_result']//div[@class='property_box']"):
            follow_url = response.urljoin(item.xpath(".//a[@title='Find out more']/@href").get())
            room_count = item.xpath(".//div[@class='property_bed']/text()[last()]").get()
            bathroom_count = item.xpath(".//div[@class='property_bath']/text()[last()]").get()
            rent = item.xpath(".//div[@class='price']/text()").get()
            rent = "".join(filter(str.isnumeric, rent.split('.')[0])) if rent else rent
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"], "room_count":room_count, "bathroom_count":bathroom_count, "rent":rent})

        next_button = response.xpath("//a[contains(@class,'-next')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Lisney_PySpider_ireland")
        externalid=response.xpath("//link[@rel='shortlink']").get()
        if externalid:
            externalid=externalid.split("=")[-1]
            externalid=re.findall("\d+",externalid)
            item_loader.add_value("external_id",externalid)
        room_count = response.meta["room_count"]
        bathroom_count = response.meta["bathroom_count"]
        rent = response.meta["rent"]
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        city = response.xpath("//div[@class='property_location']/text()").get()
        if city:
            item_loader.add_value("city",city.strip().strip(",").split(",")[-1].strip())
        
        zipcode = response.xpath("//div[@class='property_location']/span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title",re.sub("\s{2,}", " ", title).replace("\n"," "))
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address",re.sub("\s{2,}", " ", address).replace("\n"," "))
        
        desc = " ".join(response.xpath("//div[@id='id-res-desc']/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
   
        item_loader.add_xpath("latitude", "//div[@class='marker']/@data-lat")
        item_loader.add_xpath("longitude", "//div[@class='marker']/@data-lng")
        
        images = [ x for x in response.xpath("//div[@class='modal_slider gellery-slider']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)        
                
        furnished = response.xpath("//div[@id='id-res-desc']/p/text()[contains(.,'Unfurnished') or contains(.,'Furnished')]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//div[@class='property_parking']//text()[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//div[@class='property_style_image']//text()[contains(.,'Terraced')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//div[@class='property_parking']//text()[contains(.,'Garage')]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//div[@id='id-res-desc']/p/text()[contains(.,'Parking')]").get()
            if parking:
                item_loader.add_value("parking", True)
        
        balcony = response.xpath("//div[@class='property_parking']//text()[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        energy_label = response.xpath("//div[@id='property_ber_target']/img/@title").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        item_loader.add_value("landlord_name", "Lisney Dublin")
        item_loader.add_value("landlord_phone", "+353 (0)1 638 2700")
        item_loader.add_value("landlord_email", "dublin@lisney.com")

        yield item_loader.load_item()