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
    name = 'hamptonsrealestate_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.hamptonsrealestate.com/eng/hampton-rentals/southampton-southampton-town-ny-usa/apartment-type",
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "https://www.hamptonsrealestate.com/eng/hampton-rentals/southampton-southampton-town-ny-usa/multi-family-home-type/single-family-home-type/townhouse-type",
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'listing-item__image')]/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        next_page = response.xpath("//a[contains(@class,'paging__item--next ')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type')})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Hamptonsrealestate_PySpider_united_kingdom")

        item_loader.add_css("title", "title")
        
        address = response.xpath("//h3//span[@class='address']/text()").get()
        city = response.xpath("//h3//span[@class='locality']/text()").get()
        state = response.xpath("//h3//span[@class='region']/text()").get()
        zipcode = response.xpath("//h3//span[@class='postal-code']/text()").get()
        
        item_loader.add_value("address", f"{address} {city} {state} {zipcode}".strip())
        if city:
            item_loader.add_value("city", city)
        item_loader.add_value("zipcode", f"{state} {zipcode}".strip())
        
        rent = response.xpath("//div[contains(@class,'price')]/text()[contains(.,'$')]").get()
        if rent:
            rent = rent.split("$")[1].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")
        
        room_count = response.xpath("//dl[contains(.,'Bedroom')]/dd/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//dl[contains(.,'Baths')]/dd/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        square_meters = response.xpath("//dl[contains(.,'Sq')]/dd/text()").get()
        if square_meters:
            square_meters = str(int(int(square_meters.split(" ")[0].replace(",",""))* 0.09290304))
        
        swimming_pool = response.xpath("//li[contains(.,'Pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        description = " ".join(response.xpath("//div[@class='p']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
            
        images = [x for x in response.xpath("//img[@class='photo']/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'Long')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('Latitude:"')[1].split('"')[0]
            longitude = latitude_longitude.split('Longitude:"')[1].split('"')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "Hamptons Real Estate")
        item_loader.add_value("landlord_phone", "(631) 288-4800")
        
        yield item_loader.load_item()