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
import scrapy

class MySpider(Spider):
    name = 'locanto_ie'
    execution_type='testing' 
    country='ireland'
    locale='en'
    external_source = "Locanto_PySpider_ireland"
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
        "HTTPERROR_ALLOWED_CODES": [403]
    } 
    headers={
        "Referer": "https://www.locanto.ie/Property/R/?type=rent",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Mobile Safari/537.36"
    }

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.locanto.ie/Flats-for-Rent/301/', 'property_type': 'apartment'},
            {'url': 'https://www.locanto.ie/Houses-for-Rent/307/', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse,headers=self.headers,
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 1)
        border=3
        seen = False
        for url in response.xpath("//a[@class='bp_ad__link']/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item,meta={'property_type': response.meta.get('property_type')})
            seen = True
        if border:
            if page<=int(border)+3:
                if page==1 or seen: 
                    nextpage=f"https://www.locanto.ie/Flats-for-Rent/301/{page}"
                    if nextpage:
                        yield Request(
                            nextpage,
                            callback=self.parse,
                            dont_filter=True,
                            meta={'property_type': response.meta.get('property_type'),"page":page+1})
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        property_type = response.meta.get('property_type')
        if property_type:
            item_loader.add_value("property_type",property_type)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//strong[.='Price']/following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1].split(".")[0].replace(",",""))
        item_loader.add_value("currency","EUR")
        external_id=response.xpath("//div[@class='vap_ad_id']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split("ID:")[1])
        images=[x for x in response.xpath("//img[@id='big_img']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description=response.xpath("//div[@id='js-user_content']/text()").get()
        if description:
            item_loader.add_value("description",description)
        external_id=response.xpath("//div[@id='vap_ad_id']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        property_type=response.xpath("//span[@class='header-text']/text()").get()
        if property_type and "house" in property_type.lower():
            item_loader.add_value("property_type","house")
        if property_type and "apartment" in property_type.lower():
            item_loader.add_value("property_type","house")
        if property_type and "room" in property_type.lower():
            item_loader.add_value("property_type","room")
        latitude=response.xpath("//span[@itemprop='latitude']/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//span[@itemprop='longitude']/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude)
        item_loader.add_value("landlord_name","Locanto")
        item_loader.add_value("city","Dublin")


        yield item_loader.load_item()