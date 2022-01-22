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
import math
class MySpider(Spider):
    name = 'easyrenting_pl'
    execution_type='testing'
    country='poland'
    locale='pl'
    external_source = "Easyrenting_PySpider_poland"
 
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.easyrenting.pl/search-for-apartment/page/1"  
                ],         
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={"property_type": url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//li/article/header"):
            rented = item.xpath(".//div[@class='badge rented']").get()
            if rented and "rented" in rented.lower():
                continue
            else:
                follow_url = item.xpath(".//div[@class='hover-icon-wrapper']/a/@href").get()
                prop = item.xpath(".//a[@class='property-type']/text()").get()
                if prop and "studio" in prop.lower():
                    property_type = "studio"
                else:
                    property_type = "apartment"
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": property_type})
        
        pagination = response.xpath("//ul/li[@class='bon-next-link']/a/@href").get()
        if pagination: 
            yield Request(response.urljoin(pagination), callback=self.parse)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("property_type", response.meta.get("property_type"))

        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            external_id = external_id.split("/?p=")[-1]
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//h1[@class='entry-title']/text()").get()
        if title:
            item_loader.add_value("title", title)

        rent = response.xpath("//h4//span[@itemprop='price']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace(",", ""))
        item_loader.add_value("currency", "PLN")
        
        if response.meta.get("property_type") == "studio":
            item_loader.add_value("room_count", 1)
        else:
            room_count = response.xpath("//span[@class='meta-value']/text()[contains(.,'Room')]").get()
            if room_count:
                item_loader.add_value("room_count", room_count.lstrip().replace("Rooms", "").replace("Room", "").strip())
            else:
                room_count = response.xpath("//span[@class='meta-value']/text()[contains(.,'Bed')]").get()
                if room_count:
                    item_loader.add_value("room_count", room_count.replace("Bed", "").strip())

        bathroom = response.xpath("//span[@class='meta-value']/text()[contains(.,'Bath')]").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.replace("Baths", "").replace("Bath", "").strip())

        square_meters = response.xpath("//span[@class='meta-value']/text()[contains(.,'sq')]").get()
        if square_meters and "sq ft" in square_meters:
            sqm = square_meters.replace("sq", "").replace("ft", "").replace("\n", "").replace("\t", "").strip().split(".")[0]
            sqm = str(int(sqm)* 0.09290304)
            item_loader.add_value("square_meters", sqm.split(".")[0])

        city = response.xpath("(//strong[contains(.,'Location')]/following-sibling::span/a/text())[1]").get()
        if city:
            item_loader.add_value("city", city)

        address = response.xpath("//strong[contains(.,'Address')]/following-sibling::span/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        else:
            address = city
            item_loader.add_value("address", address.strip())

        available_date = response.xpath("//strong[contains(.,'Date')]/following-sibling::span/text()").get()
        if available_date:
            item_loader.add_value("available_date", available_date.strip())

        description = "".join(response.xpath("//div[@itemprop='description']/p/text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.replace("\r","").replace("\n","").strip())
            item_loader.add_value("description", description)

        images = [x for x in response.xpath("//ul[@class='bxslider']/li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Easy Renting")
        item_loader.add_value("landlord_phone", "+48 790 555 175")
        item_loader.add_value("landlord_email", "erasmus@easyrenting.pl")
        
        yield item_loader.load_item()