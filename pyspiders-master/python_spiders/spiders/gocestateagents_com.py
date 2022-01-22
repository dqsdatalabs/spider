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
    name = 'gocestateagents_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'   
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.gocestateagents.com/propertysearch/419389/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.gocestateagents.com/propertysearch/419390/",
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
        item_loader.add_value("external_source", "Gocestateagents_PySpider_united_kingdom") 
        item_loader.add_xpath("title","//h1/text()")
        item_loader.add_xpath("room_count", "//li[span[.='Bedrooms']]/span[2]/text()")
        item_loader.add_xpath("rent_string", "//li[span[.='Price']]/span[2]//text()")
        address =response.xpath("//h1/text()").get()
        if address:
            address = address.replace("  ","").strip().replace(",,",",")
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            
        available_date = response.xpath("//li[span[.='Available From']]/span[2]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'PARKING')]/text() | //li[contains(.,' Garage')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        terrace = response.xpath("//li[span[.='Style']]/span[2]/text()[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        balcony = response.xpath("//li[contains(.,'balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        furnished = response.xpath("//li[contains(.,'furnished') or contains(.,'Furnished') or contains(.,'FURNISHED')]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        description = " ".join(response.xpath("//h2[.='Description']/following-sibling::div[1]//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        script_map = response.xpath("//script[contains(.,'lat:')]/text()[not(contains(.,'lat: results[0]'))]").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("lat:")[1].split(",")[0].strip())
            item_loader.add_value("longitude", script_map.split("lng:")[1].split("}")[0].strip())

        images = [response.urljoin(x) for x in response.xpath("//ul[@id='gallery']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        landlord_name = " ".join(response.xpath("//div[@class='detag']//span[@class='name']/text()[normalize-space()]").getall())
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        item_loader.add_xpath("landlord_phone", "//div[@class='detag']//span[@class='ph']/a/text()")
        item_loader.add_value("landlord_email", "info@gocestateagents.com")
        yield item_loader.load_item()