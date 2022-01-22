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
    name = 'immocnc_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Immocnc_PySpider_belgium'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://immocnc.be/nl/te-huur?view=list&page=1&ptype=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://immocnc.be/nl/te-huur?view=list&page=1&ptype=1",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url["url"]:
                yield Request(item,callback=self.parse,meta={'property_type': url['property_type']})
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='image']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        nextpage=response.xpath("//a[.='>|']/@href").get()
        if nextpage:
            yield Request(response.urljoin(nextpage),callback=self.parse, meta={"property_type":response.meta["property_type"]})
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        title=response.xpath("//h3[@class='pull-left leftside']/text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//div[.='Prijs']/following-sibling::div[@class='value']/text()").get()
        if rent and not "op" in rent.lower():
            item_loader.add_value("rent",rent.split("€")[-1].strip())
        item_loader.add_value("currency","EUR")
        images=[x for x in response.xpath("//li[@class='last page-1 thumbnail']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description=response.xpath("//div[@class='content']//div[@class='field']/text()").get()
        if description:
            item_loader.add_value("description",description)
        adres=response.xpath("//div[.='Adres']/following-sibling::div[@class='value']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        square_meters=response.xpath("//div[.='Bewoonbare opp.']/following-sibling::div[@class='value']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        room_count=response.xpath("//div[.='Aantal slaapkamers']/following-sibling::div[@class='value']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count=response.xpath("//div[.='Aantal douchekamers']/following-sibling::div[@class='value']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        yield item_loader.load_item() 