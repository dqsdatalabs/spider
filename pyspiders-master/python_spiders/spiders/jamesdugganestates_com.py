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
class MySpider(Spider):
    name = 'jamesdugganestates_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'   

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.jamesdugganestates.com/search?sta=toLet&st=rent&pt=residential&stygrp=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.jamesdugganestates.com/search?sta=toLet&st=rent&pt=residential&stygrp=2&stygrp=10&stygrp=8&stygrp=9&stygrp=6",
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

        for item in response.xpath("//ul[@class='property-list']/li//a[contains(.,'More Details')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='paging-next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_value("external_source", "Jamesdugganestates_PySpider_united_kingdom")          
        item_loader.add_xpath("title","//title/text()")
        item_loader.add_xpath("room_count", "//tr[th[.='Bedrooms']]/td/text()")
        item_loader.add_xpath("bathroom_count", "//tr[th[.='Bathrooms']]/td/text()")
        item_loader.add_xpath("address", "//tr[th[.='Address']]/td/text()")
        item_loader.add_xpath("city", "//span[@class='addr-town']/text()")
        zipcode = " ".join(response.xpath("//span[@class='addr-postcode']/span/text()").getall()) 
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        available_date = response.xpath("//tr[th[.='Available From']]/td/text()").get()
        if available_date:
            item_loader.add_value("available_date", dateparser.parse(available_date.strip(), languages=['en']).strftime("%Y-%m-%d"))
      
        rent_string = " ".join(response.xpath("//tr[th[.='Rent']]/td//text()").getall()) 
        if rent_string:
            item_loader.add_value("rent_string", rent_string.strip())
        terrace = response.xpath("//tr[th[.='Style']]/td/text()[contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        energy_label = response.xpath("//tr[th[.='EPC Rating']]/td//a/text()[contains(.,'/')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label[0])
  
        floor = response.xpath("//tr[th[.='Style']]/td/text()[contains(.,'Floor ')]").get()
        if floor:
            item_loader.add_value("floor", floor.split("Floor ")[0].strip())
        description = " ".join(response.xpath("//section[@class='listing-additional-info']/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
        
        deposit = response.xpath("//tr[th[.='Deposit']]/td/text()").get()
        if deposit:
            deposit = deposit.split("£")[1].replace(",","")
            item_loader.add_value("deposit", int(float(deposit)) )
        furnished = response.xpath("//tr[th[.='Furnished']]/td/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        item_loader.add_xpath("latitude", "//meta[@property='og:latitude']/@content")
        item_loader.add_xpath("longitude", "//meta[@property='og:longitude']/@content")
        images = [x for x in response.xpath("//div[@class='carousel carousel-stage']//li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "James Duggan Estates")
        item_loader.add_value("landlord_phone", "028 9092 5050")
        item_loader.add_value("landlord_email", "info@jamesdugganestates.com")
        yield item_loader.load_item()