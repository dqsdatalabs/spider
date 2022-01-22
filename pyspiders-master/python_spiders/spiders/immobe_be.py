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
from python_spiders.helper import extract_number_only, remove_white_spaces



class MySpider(Spider):
    name = 'immobe_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source = "Immobe_PySpider_belgium"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.immobe.be/brussels-1bedroom.asp",
                    "https://www.immobe.be/brussels-2bedrooms.asp",
                    "https://www.immobe.be/brussels-3bedrooms.asp"
                ],
                "property_type" : "apartment",
         
            },
            {
                "url" : [
                    "https://www.immobe.be/brussels-houses.asp",
                ],
                "property_type" : "house",
                
            },
            {
                "url" : [
                    "https://www.immobe.be/brussels-studios.asp",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item,})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='arrowgreyu']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)   

        title=response.xpath("//div[@class='appartment']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//div[@class='appartment']/h2/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        images=[x for x in response.xpath("//img[@class='image0']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description=response.xpath("//div[@class='apptxt']/p/text()").getall()
        if description:
            item_loader.add_value("description",description)
        rent="".join(response.xpath("//div[@class='lfttxtDiv']/h3//text()").getall())
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].split("Rate From")[-1])
        

        item_loader.add_value("currency","EUR")
        item_loader.add_value("landlord_name","IMMOBEBE IMMOBILIER")
        item_loader.add_value("landlord_phone"," +32 (0)2 227 64 80")


        yield item_loader.load_item()