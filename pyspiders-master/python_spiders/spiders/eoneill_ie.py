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
import dateparser
class MySpider(Spider):
    name = 'eoneill_ie'
    execution_type='testing'
    country='ireland'
    locale='en'
    external_source="Eoneill_PySpider_ireland"
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://property.eoneill.ie/rentals/results?status=7%7c8%7c11",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        for item in  response.xpath("//li[@class='resultItem']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            status=item.xpath(".//div[@class='price']/text()").get()
            if status and "Let Agreed" in status:
                continue 
            yield Request(follow_url, callback=self.populate_item)
        nextpage=response.xpath("//a[.='»']/@href").get()
        if nextpage:
            yield Request(response.urljoin(nextpage), callback=self.parse)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//h1[@class='col-xs-12']/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ',title.strip()))
        adres=response.xpath("//h1[@class='col-xs-12']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        item_loader.add_value("city","Dublin")
        rent=response.xpath("//h1[@class='col-xs-12']/span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1].split("/")[0].replace(",",""))
        property_type=response.xpath("//i[@class='fa fa-home']/following-sibling::text()").get()
        if property_type and("Terraced" in property_type or "Semi-Detached" in property_type):
            item_loader.add_value("property_type","house")
        room_count=response.xpath("//i[@class='fa fa-bed']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("Bed")[0].strip())
        bathroom_count=response.xpath("//i[@class='fa fa-bath']/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("Bath")[0].strip())
        description="".join(response.xpath("//h3[.='Description']/following-sibling::p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//a[@class='swipebox']/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        latitude=response.xpath("//input[@name='propertyLat']/@value").get()
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//input[@name='propertyLng']/@value").get()
        if longitude:
            item_loader.add_value("longitude",longitude)
        item_loader.add_value("landlord_phone","01 660 0333 ")
        item_loader.add_value("landlord_name","Eoin O’Neill")

        yield item_loader.load_item()