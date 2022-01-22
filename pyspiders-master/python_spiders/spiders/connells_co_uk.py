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

class MySpider(Spider): 
    name = 'connells_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Connells_PySpider_united_kingdom"
    custom_settings = {
        "HTTPCACHE_ENABLED":False
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.connells.co.uk/properties/lettings/tag-flat",
                ],
                "property_type" : "apartment",
            },

            {
                "url" : [
                    "https://www.connells.co.uk/properties/lettings/tag-house",
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
        page = response.meta.get('page', 2)
        seen = False

        for item in response.xpath("//div[@class='property ']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        if page == 2 or seen:
            next_page = f"{response.url}/page-{page}"
            if next_page:
                yield Request(url=response.urljoin(next_page),callback=self.parse,meta={"page": page+1,"property_type" : response.meta.get("property_type")}) 

    
    # 2. SCRAPING level 2 
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)

        item_loader.add_xpath("title","//title//text()")
        external_id=response.xpath("//div[contains(.,'Property ref:')]/text()/following-sibling::b/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)

        room_count=response.xpath("//span[contains(.,'Bedroom')]/preceding-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[contains(.,'Bathroom')]/preceding-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        rent_text=response.xpath("//script[contains(text(),'sales_price_dropdown_list')]/text()").get()
        if rent_text:
            rent = re.search('sales_price":"£([\d]+) ', rent_text)
            if rent:
                item_loader.add_value("rent",rent.group(1))
        item_loader.add_value("currency","EUR")

        address=response.xpath("//h2[@class='property-hero__address ']/text()").get()
        if address:
            if len(address.strip()) > 2:
                item_loader.add_value("address",address.strip())
            else:
                address=response.xpath("//h1[@class='property-hero__street mb--8']/text()").get()
                if address:
                    item_loader.add_value("address",address.strip())
        else:
            address=response.xpath("//h1[@class='property-hero__street mb--8']/text()").get()
            if address:
                item_loader.add_value("address",address.strip())
        description=response.xpath("//div[@class='toggler--area mb--16']/div[2]/text()").get()
        if description:
            item_loader.add_value("description",description.strip())
        else:
            description = " ".join(response.xpath("//div[@class='toggler--area mb--16']/div[2]/p/text()").getall())
            if description:
                item_loader.add_value("description",description)
        images=[x for x in response.xpath("//div[contains(@class,'property-hero-carousel__slide')]/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        energy_label=response.xpath("//li[contains(.,'EPC')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split(":")[-1].split(" ")[1])
        furnished=response.xpath("//li[contains(.,'FURNISHED')]/text()").get()
        if furnished:
            item_loader.add_value("furnished",True)
        terrace=response.xpath("//li[contains(.,'GARDEN')]/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        latitude="".join(response.xpath("//comment()").getall())
        if latitude:
            item_loader.add_value("latitude",latitude.split("latitude")[-1].split("-->")[0].replace(":","").replace('"',""))
        longitude="".join(response.xpath("//comment()").getall())
        if longitude:
            item_loader.add_value("longitude",longitude.split("longitude")[-1].split("-->")[0].replace(":","").replace('"',""))
        item_loader.add_value("landlord_name","Connells")
        landlord_phone=response.xpath("//span[@class='branch-property-card__callnumber']/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone",landlord_phone)
        else:
            item_loader.add_value("landlord_phone","01525 218500")

        city = response.xpath("//h2[@class='property-hero__address ']/text()").get()
        if city:
            item_loader.add_value("city",city.strip())

        yield item_loader.load_item()