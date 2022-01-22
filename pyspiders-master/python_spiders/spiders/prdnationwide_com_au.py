# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from typing import NewType
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
    name = 'prdnationwide_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Prdnationwide_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "http://prdnationwide.com.au/corporate-search/?listing_type=Lease&property_status=Available&page=1&1641900663845=&1641900672913=&1641900680419=&1641900734595=&1641900742426"}
        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='property-card__link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen=True
        if page == 2 or seen:
            nextpage=f"http://prdnationwide.com.au/corporate-search/?listing_type=Lease&property_status=Available&page={page}&1641900663845=&1641900672913=&1641900680419=&1641900734595=&1641900742426"
            if nextpage:
                yield Request(nextpage, callback=self.parse,meta={'page':page+1})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        adres="".join(response.xpath("//h1[@class='listing-details__address']/text()").get())
        if adres:
            item_loader.add_value("address",adres)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//h2[@class='listing-details__price']/span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("$")[-1].split("per")[0].replace(" ","").replace("\xa0","").replace(".","").replace(",",""))
        item_loader.add_value("currency","USD")
        description="".join(response.xpath("//div[@class='listing__description-container']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//pictue//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)

        name=response.xpath("//h3[@class='listing-agent-card__name']/a/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//a[contains(@href,'tel')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        email=response.xpath("//a[contains(@href,'mailto')]/text()").get()
        if email:
            item_loader.add_value("landlord_email",email)


        yield item_loader.load_item()