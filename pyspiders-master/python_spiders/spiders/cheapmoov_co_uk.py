# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'cheapmoov_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = 'Cheapmoov_Co_PySpider_united_kingdom'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://cheapmoov.co.uk/listings/all/mode/rent/type/51",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://cheapmoov.co.uk/listings/all/mode/rent/type/52",
                    "https://cheapmoov.co.uk/listings/all/mode/rent/type/1",
                    "https://cheapmoov.co.uk/listings/all/mode/rent/type/50",
                    "https://cheapmoov.co.uk/listings/all/mode/rent/type/39",
                    "https://cheapmoov.co.uk/listings/all/mode/rent/type/16",
                    "https://cheapmoov.co.uk/listings/all/mode/rent/type/20"
                ],
                "property_type": "house"
            }
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
        for item in response.xpath("//ul[@id='search-list']/li"):
            url = item.xpath(".//div[@class='search-property-utility']/a/@href").get()
            let = item.xpath(".//div[@class='let-ribbon']/text()[.='Let']").get()
            if let:
                continue            
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
  
        next_page = response.xpath("//li/a[.='>>']/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Cheapmoov_Co_PySpider_united_kingdom")

        external_id = response.xpath("//span[contains(.,'Reference')]//parent::li/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        title = response.xpath("//div[contains(@class,'property-title')]//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//span[contains(.,'Location')]//parent::li/text()").get()
        if address:
            item_loader.add_value("address", address)

        city = response.xpath("//span[contains(.,'Location')]//parent::li/text()").get()
        if city:
            item_loader.add_value("city", city.split(",")[0].strip())

        rent = response.xpath("//span[contains(.,'Price')]//parent::li//b/text()").get()
        if rent:
            rent = rent.replace("£","").strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//h2[contains(.,'Property Features')]//following-sibling::ul//li[contains(.,'Deposit')]//text()").get()
        if deposit:
            deposit = deposit.split(":")[1].replace("£","").strip()
            item_loader.add_value("deposit", deposit)
        else:
            deposit = response.xpath("//div[contains(@class,'property-text')]//text()[contains(.,'deposit')]").get()
            if deposit:
                deposit = deposit.split("deposit")[0].split("£")[-1].strip().split(" ")[0]
                item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@class,'property-text')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[contains(.,'Bed')]//parent::li/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//span[contains(.,'Bath')]//parent::li/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'detail-page-slide')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        balcony = response.xpath("//h2[contains(.,'Property Features')]//following-sibling::ul//li[contains(.,'balcon') or contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//h2[contains(.,'Property Features')]//following-sibling::ul//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        location=response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if location:
            lat=location.split("google.maps.LatLng")[-1].split(")")[0]
            item_loader.add_value("latitude",lat.split(",")[0].replace("(",""))
            lon=location.split("google.maps.LatLng")[-1].split(")")[0]
            item_loader.add_value("longitude",lon.split(",")[-1])

        item_loader.add_value("landlord_name", "CHEAPMOOV")
        item_loader.add_value("landlord_phone", "0151 433 2900")
        item_loader.add_value("landlord_email", "info@cheapmoov.co.uk")
   
        yield item_loader.load_item()