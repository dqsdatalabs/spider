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
    name = 'caprent_com'
    execution_type='testing' 
    country='ireland'
    locale='en'
    external_source = "Caprent_com_PySpider_ireland"

    def start_requests(self):
        url = "https://www.caprent.com/ireland/"
        yield Request(url, callback=self.parse,)   

    # 1. FOLLOWING
    def parse(self, response):
        for url in response.xpath("//div[@class='col-sm-4']//ul//li//a//@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=",".join(response.xpath("//div[@itemprop='address']/p/span//text()").getall())
        if adres:
            item_loader.add_value("address",adres)
        item_loader.add_value("city","Dublin")
        description=response.xpath("//p[@class='sub-title']/following-sibling::p/text()").get()
        if description:
            item_loader.add_value("description",description)
        images=[x.replace(" ","%20") for x in response.xpath("//ul//li//img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        latitude=response.xpath("//script[contains(.,'google.maps.Map')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("beachMarker")[-1].split("});")[0].split("lat")[-1].split(",")[0].replace(":","").strip())
        longitude=response.xpath("//script[contains(.,'google.maps.Map')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("beachMarker")[-1].split("});")[0].split("lng")[-1].split(",")[0].replace(":","").strip())
        features=response.xpath("//h2[.='Building Features']/following-sibling::ul//li//text()").getall()
        for i in features:
            if "parking" in i:
                item_loader.add_value("parking",True)
            if "garden" in i:
                item_loader.add_value("terrace",True)


        yield item_loader.load_item()