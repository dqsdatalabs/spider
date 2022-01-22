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
    name = 'oakmans_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Oakmans_PySpider_united_kingdom"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.oakmans.co.uk/buying/?department=residential-lettings",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='properties card-deck']//a//@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        nextpage=response.xpath("//a[@class='next page-numbers page-link']/@href").get()
        if nextpage:
            yield Request(nextpage, callback=self.parse)
            
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        adres=response.xpath("//div[@id='content']/div/h1/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        external_id=response.xpath("//div[@id='content']/div/h1/following-sibling::p/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split("REF")[-1].strip())
        rent=response.xpath("//h3[@class='card-header bg-primary']/small/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("£")[-1].split("pcm")[0].replace(",",""))
        item_loader.add_value("currency","GBP")
        feautures=response.xpath("//ul[@class='list-features']//li//text()").getall()
        for i in feautures:
            if "Parking" in i:
                item_loader.add_value("parking",True)
            if "Unfurnish" in i:
                item_loader.add_value("furnished",False)
            if "Bedroom" in i:
                room=i.split(" ")[0]
                if "one" in room.lower():
                    item_loader.add_value("room_count","1")
                if "two" in room.lower():
                    item_loader.add_value("room_count","2")
                if "three" in room.lower():
                    item_loader.add_value("room_count","3")
            
        images=[x for x in response.xpath("//a[@class='img-fit']/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description=response.xpath("//h4[.='Description']/following-sibling::p/text()").get()
        if description:
            item_loader.add_value("description",description)
        latitude=response.xpath("//script[contains(.,'google.maps.LatLng(')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("google.maps.LatLng(")[-1].split(",")[0])
        longitude=response.xpath("//script[contains(.,'google.maps.LatLng(')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("google.maps.LatLng(")[-1].split(");")[0].split(",")[1])
        item_loader.add_value("landlord_name","Oakmans ESTATE Agents")

        yield item_loader.load_item()