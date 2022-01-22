# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from parsel.utils import extract_regex
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
    name = 'craigmoreelizabeth_ljhooker_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='CraigmoreelizabethLjhooker_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://craigmoreelizabeth.ljhooker.com.au/search/property-for-rent/page-1"},

        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='track-link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen=True
        if page == 2 or seen:
            nextpage=response.xpath("//ul[@class='pagination']//li//a[@aria-label='Next']/@href").get()
            if nextpage:
                yield Request(response.urljoin(nextpage), callback=self.parse)
                
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//div[@class='property-heading']/h1/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        rent=response.xpath("//div[@class='property-heading']/h2/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("$")[-1].split("Per")[0].split(".")[0])
        item_loader.add_value("currency","USD")
        room_count=response.xpath("//span[@class='bed ']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[@class='bathroom ']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        parking=response.xpath("//span[@class='carpot ']/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        description="".join(response.xpath("//div[@class='property-text is-collapse-disabled']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//div/span/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        property_type=response.xpath("//strong[.='Property Type:']/following-sibling::text()").get()
        if property_type:
            item_loader.add_value("property_type",property_type.strip())
        available_date=response.xpath("//strong[.='Date Available:']/following-sibling::text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)
        square_meters=response.xpath("//strong[.='Land Area:']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        item_loader.add_value("landlord_name","LJ Hooker Craigmore | Elizabeth")
        item_loader.add_value("landlord_phone","(08) 8255 9555")

        yield item_loader.load_item()