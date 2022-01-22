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
    name = 'agenceducardinal_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Agenceducardinal_PySpider_france"
    start_urls = ['https://www.agenceducardinal.fr/en/search/']  # LEVEL 1

    formdata = {
        "nature": "2",
        "type[]": "1",
        "price": "",
        "age": "",
        "tenant_min":"",
        "tenant_max":"" ,
        "rent_type": "",
        "currency": "EUR",
        "homepage": "",
    }

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "1",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "2"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                self.formdata["type[]"] = item
                yield FormRequest(
                    url=self.start_urls[0],
                    formdata=self.formdata,
                    dont_filter=True,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//li[@class='ad']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)


        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title.replace("\u00e8","").replace("\u00b2",""))

        external_id=response.xpath("//span[@class='reference']//text()").get()
        if external_id:
            external_id=external_id.split("Ref.")[1]
            item_loader.add_value("external_id",external_id)   

        room_count=response.xpath("//li[contains(.,'Rooms')]//span//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("room"))

        floor=response.xpath("//li[contains(.,'Floor')]//span//text()").get()
        if floor:
            item_loader.add_value("floor",floor.split("/")[1].split("floors")[0])

        square_meters=response.xpath("//li[contains(.,'Total area')]//span//text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip()
            item_loader.add_value("square_meters",square_meters)

        rent=response.xpath("//h2[@class='price']//text()").get()
        if  rent:
            rent = rent.split("€")[0].strip()
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        utilities=response.xpath("//li[contains(.,'Fees')]//span//text()").get()
        if  utilities:
            item_loader.add_value("utilities",utilities)

        images=[response.urljoin(x) for x in response.xpath("//div[@class='show-carousel owl-carousel owl-theme']//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)

        item_loader.add_value("landlord_phone", "+33 (0)1 34 62 11 72")
        item_loader.add_value("landlord_name", "AGENCE DU CARDINAL")

        yield item_loader.load_item()