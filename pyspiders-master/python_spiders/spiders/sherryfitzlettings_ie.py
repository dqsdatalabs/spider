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
    name = 'sherryfitzlettings_ie'
    execution_type='testing'
    country='Ireland'
    locale='en'
    external_source = "Sherryfitzlettings_PySpider_Ireland"
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.sherryfitz.ie/rent/property?search%5Bproperty_id%5D=false&search%5Bproperty_types%5D%5B%5D=Apartment&search%5Bproperty_types%5D%5B%5D=Duplex+Apartment&search%5Btype%5D=rent",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.sherryfitz.ie/rent/property?search%5Bproperty_id%5D=false&search%5Bproperty_types%5D%5B%5D=Bungalow&search%5Bproperty_types%5D%5B%5D=Detached+house&search%5Bproperty_types%5D%5B%5D=End+terrace+house&search%5Bproperty_types%5D%5B%5D=House&search%5Bproperty_types%5D%5B%5D=Semi+detached+house&search%5Bproperty_types%5D%5B%5D=Terrace+house&search%5Btype%5D=rent",
                ],
                "property_type" : "house",
            }            
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='property-card-action']/a/@href").getall():
            follow_url = item

            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get("property_type")})
        
        next_page = response.xpath("//a[@class='pagination-arrow pagination-next']/@href").get()
        if next_page:
            follow_next = "https://www.sherryfitz.ie/" + next_page
            yield Request(follow_next, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        rent = response.xpath("//div[@class='property-price']/text()").getall()
        if rent:
            rent = "".join(rent).strip().replace(",","").replace("€","")

            item_loader.add_value("rent",rent)

        room = response.xpath("//div[@class='property-stat'][contains(.,'bed')]//text()[normalize-space()]").get()
        if room:
            room = room.strip().split()[0]
            item_loader.add_value("room_count",room)

        bathroom = response.xpath("//div[@class='property-stat'][contains(.,'bath')]//text()[normalize-space()]").get()
        if bathroom:
            bathroom = bathroom.strip().split()[0]
            item_loader.add_value("bathroom_count",bathroom)


        sqm = response.xpath("//div[@class='property-stat'][contains(.,'sqm')]//text()[normalize-space()]").get()
        if sqm:
            sqm = sqm.strip().split()[0]
            item_loader.add_value("square_meters",sqm)


        desc = " ".join(response.xpath("//div[@class='property-description']//text()").getall())
        if desc:
            item_loader.add_value("description",desc)

        images = response.xpath("//img[@class='property-inline-image']/src").getall()
        if images:
            item_loader.add_value("images",images)


        item_loader.add_value("landlord_email","amahon@sflettings.ie")
        item_loader.add_value("landlord_name","Aoife Mahon")

        title = "".join(response.xpath("//div[@class='property-address']/h1").getall())
        if title:
            item_loader.add_value("title",title)
            item_loader.add_value("address",title)

        item_loader.add_value("city","Dublin")
        item_loader.add_value("currency","EUR")
        item_loader.add_value("external_id",response.url.split("-")[-1])



        position = response.xpath("//attribute::*[contains(., 'latitu')]").get()
        if position:
            lat = re.search('"latitude":([\d.]+)', position).group(1)
            long = re.search('"longitude":([-\d.]+)', position).group(1)
            item_loader.add_value("longitude",long)
            item_loader.add_value("latitude",lat)



        yield item_loader.load_item()