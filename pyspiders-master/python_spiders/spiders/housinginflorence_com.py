# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'housinginflorence_com'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Housinginflorence_PySpider_italy"
    start_urls = ['https://www.housinginflorence.com/properties-browser?field_property_type_tid=All&field_area_tid=All&field_zone_tid=All&available%5Bfrom%5D%5Bdate%5D=&available%5Bto%5D%5Bdate%5D=']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='field-content']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": "apartment"})
        
        next_page = response.xpath("//a[contains(.,'next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": "apartment"})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//h1[contains(@class,'title')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(" ")[1])

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//div[@class='field-label'][contains(.,'Neighborhood')]//following-sibling::div//text()").get()
        if address:
            item_loader.add_value("address", address)

        city = response.xpath("//h1[contains(@class,'title')]//text()").get()
        if city:
            item_loader.add_value("city", city.split(" ")[2:3])

        description = response.xpath("//div[contains(@class,'field-item even')]//p//text()").get()
        if description:
            item_loader.add_value("description", description)

        bathroom_count = response.xpath("//div[@class='field-label'][contains(.,'Bathrooms')]//following-sibling::div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        room_count = response.xpath("//div[@class='field-label'][contains(.,'Bedrooms')]//following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        square_meters = response.xpath("//div[@class='field-label'][contains(.,'Sq.Mt')]//following-sibling::div//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        rent = response.xpath("//div[@class='field-label'][contains(.,'Price')]//following-sibling::div//text()").get()
        if rent:
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR") 

        latitude_longitude = response.xpath(
            "//script[contains(.,'latitude')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                '[{"latitude":"')[1].split('"')[0]
            longitude = latitude_longitude.split(
                ',"longitude":"')[1].split('"')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        images = [response.urljoin(x) for x in response.xpath("//div[@class='field-slideshow field-slideshow-1 effect-fade timeout-4000 with-pager with-controls']//img[contains(@class,'field-slideshow-image field-slideshow')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "housinginflorence")
        item_loader.add_value("landlord_phone", "+39 349 64 066 85")
        item_loader.add_value("landlord_email", "salas@housinginflorence.com")

        yield item_loader.load_item()