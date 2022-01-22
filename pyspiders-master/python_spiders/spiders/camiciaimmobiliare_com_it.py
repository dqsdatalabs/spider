# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'camiciaimmobiliare_com_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Camiciaimmobiliare_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.camiciaimmobiliare.com/property-type/affitto/",
                ],
                "property_type": "apartment"
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

        headers = {
            "authority": "www.camiciaimmobiliare.com",
            "path": "/property-type/affitto/",
            "referer": "https://www.camiciaimmobiliare.com/",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36"
        }

        for item in response.xpath("//div[@class='row']//article//a[@class='property-row-image']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(
                follow_url,
                headers=headers,
                dont_filter=True, 
                callback=self.populate_item, 
                meta={"property_type": response.meta.get('property_type')}
                )

    # 2. SCRAPING level 2

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//h1[contains(@class,'entry-title property-title')]//text()").get()
        if title:
            item_loader.add_value("title",title.replace("\u00a0","").replace("\u2013",""))

        rent=response.xpath("//div[@class='property-overview']//dl//dt[contains(.,'Prezzo')]//following-sibling::dd[1]//text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1])
        item_loader.add_value("currency","EUR")

        address=response.xpath("//div[@class='property-overview']//dl//dt[contains(.,'Zona')]//following-sibling::dd[1]//text()").get()
        if address:
            item_loader.add_value("address",address)

        room_count=response.xpath("//div[@class='property-overview']//dl//dt[contains(.,'Vani')]//following-sibling::dd[1]//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        bathroom_count=response.xpath("//div[@class='property-overview']//dl//dt[contains(.,'Bagni')]//following-sibling::dd[1]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        description=response.xpath("//div[@class='property-description']//p//text()").getall()
        if description:
            item_loader.add_value("description",description)

        images = [response.urljoin(x)for x in response.xpath("//div[contains(@class,'property-detail-gallery')]//img//@src").extract()]
        if images:
                item_loader.add_value("images", images)


        latitude = response.xpath("//div[contains(@class,'property-map-position')]//div//@data-latitude").get()
        if latitude:
            item_loader.add_value("latitude", latitude)

        longitude = response.xpath("//div[contains(@class,'property-map-position')]//div//@data-longitude").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        item_loader.add_value("landlord_phone", "06.33610974 - 06.33612210")
        item_loader.add_value("landlord_email", "alvatore.camicia@alice.it")
        item_loader.add_value("landlord_name", "Camicia Immobiliare")
        
        yield item_loader.load_item()
