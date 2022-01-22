# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'gilbertpierreimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Gilbertpierreimmobilier_PySpider_france_fr"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.gilbertpierreimmobilier.com/annonces/?status=location&type=appartement",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.gilbertpierreimmobilier.com/annonces/?status=location&type=maison"
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
        
        page = response.meta.get('page', 2)
        property_type = response.meta.get("property_type")
        
        seen = False
        for item in response.xpath("//a[@class='btn-default']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True

        if property_type and "apartment" in property_type.lower():
            if page == 2 or seen:
                url = f"https://www.gilbertpierreimmobilier.com/annonces/page/{page}/?status=location&type=appartement#038;type=appartement"
                yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//h2[@class='page-title']/text()").get()
        if title:
            item_loader.add_value("title",title) 

        external_id = response.xpath("//span[contains(.,'Référence')]//following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)

        description = response.xpath("//div[contains(@class,'property-content')]//p/text()").get()
        if description:
            item_loader.add_value("description",description)

        rent = response.xpath("//span[contains(@class,'single-property-price price')]//text()").get()
        if rent:
            rent = rent.split("€")[0]
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        square_meters = response.xpath("//span[contains(.,'Surface')]//following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)

        room_count = response.xpath("//span[contains(.,'Chambres')]//following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count) 
        else:
            room_count = response.xpath("//span[contains(.,'Pièces')]//following-sibling::span/text()").get()
            if room_count:
                item_loader.add_value("room_count",room_count) 

        images = [x for x in response.xpath("//ul[contains(@class,'slides')]//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        landlord_phone = response.xpath("(//ul[@class='agent-contacts-list']//li[@class='office']/text())[1]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_name = response.xpath("(//h3[@class='agent-name']//a/text())[1]").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        yield item_loader.load_item()