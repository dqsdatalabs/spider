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
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'pmresidential_com'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.pmresidential.com/properties/?form-group_price-to=&filter-contract=RENT&filter-property-type=129&filter-location=&filter-price-from=&form-group+price-to=&filter-beds=&filter-baths=&filter-garages=",
                    "https://www.pmresidential.com/properties/?form-group_price-to=&filter-contract=RENT&filter-property-type=13&filter-location=&filter-price-from=&form-group+price-to=&filter-beds=&filter-baths=&filter-garages=",
                    "https://www.pmresidential.com/properties/?form-group_price-to=&filter-contract=RENT&filter-property-type=139&filter-location=&filter-price-from=&form-group+price-to=&filter-beds=&filter-baths=&filter-garages=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.pmresidential.com/properties/?form-group_price-to=&filter-contract=RENT&filter-property-type=34&filter-location=&filter-price-from=&form-group+price-to=&filter-beds=&filter-baths=&filter-garages=",
                    "https://www.pmresidential.com/properties/?form-group_price-to=&filter-contract=RENT&filter-property-type=64&filter-location=&filter-price-from=&form-group+price-to=&filter-beds=&filter-baths=&filter-garages=",
                    "https://www.pmresidential.com/properties/?form-group_price-to=&filter-contract=RENT&filter-property-type=136&filter-location=&filter-price-from=&form-group+price-to=&filter-beds=&filter-baths=&filter-garages=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'show-detail')]/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Pmresidential_PySpider_australia")      
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.split("|")[0])
            if "unfurnished" in title.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in title.lower():
                item_loader.add_value("furnished", True)

        room_count = response.xpath("//span[i[@class='pp pp-normal-bed']]/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
      
        bathroom_count = response.xpath("//span[i[@class='pp pp-normal-shower']]/strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("p=")[1].strip())

        parking = response.xpath("//span[i[@class='pp pp-normal-car']]/strong/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        address = " ".join(response.xpath("//h1[@class='entry-title property-title']//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            city = address.split(",")[-1].strip()
            item_loader.add_value("city", city)
        rent = response.xpath("//div[contains(@class,'property_price')]/strong/text()").get()
        if rent:
            rent = rent.split("$")[-1].strip().split("per")[0]
            item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", "AUD")
        desc = " ".join(response.xpath("//div[@class='property-description']/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
    
        images = [x for x in response.xpath("//div[@id='sync1']//div[@class='item']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
                    
        item_loader.add_xpath("landlord_name", "//div[@class='agent_details'][1]//div[@class='a-title']/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='agent_details'][1]//div[@class='a-email']/a/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='agent_details'][1]//div[@class='a-phone']/a/text()")
        yield item_loader.load_item()
