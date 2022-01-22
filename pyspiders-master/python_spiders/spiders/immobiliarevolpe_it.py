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
    name = 'immobiliarevolpe_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Immobiliarevolpe_Pyspider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.immobiliarevolpe.it/immobili/affitto/",
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

        for item in response.xpath("//div[@class='row mt-5']//div[@id='column-left']//a//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)


        external_id=response.xpath("//p[@class='lead']//span//text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split("rif."))

        title=response.xpath("//h1[@class='text-uppercase']//text()").get()
        if title:
            item_loader.add_value("title",title.replace("\u00a0",""))

        address=response.xpath("//p[@class='lead']//span//following-sibling::text()").get()
        if address:
            address="".join(address.split(" ")[-3:-1])
            item_loader.add_value("address",address)

        rent=response.xpath("//p[@class='h1']//span//text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1].split("al")[0])
        item_loader.add_value("currency","EUR")
        parking=response.xpath("//li[contains(.,'box')]").get()
        if parking:
            item_loader.add_value("parking",True)
        elevator=response.xpath("//li[contains(.,'ascensore')]").get()
        if elevator:
            item_loader.add_value("elevator",True)
        furnished=response.xpath("//li[contains(.,'arredato')]").get()
        if furnished:
            item_loader.add_value("furnished",True)
        balcony=response.xpath("//li[contains(.,'balconi')]").get()
        if balcony:
            item_loader.add_value("balcony",True)
        city=response.xpath("//p[@class='lead']/span/following-sibling::text()").get()
        if city:
            item_loader.add_value("city",city.split(",")[0].split(" ")[-1])

        square_meters=response.xpath("//li[@class='col-md-4']//text()[contains(.,'mq')]").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("mq")[0])

        room_count=response.xpath("//li[@class='col-md-4']//text()[contains(.,'piano')]").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("piano")[1])

        description=response.xpath("//p[@class='text-muted']//following-sibling::p//text() | //p[@class='text-muted']/text()").getall()
        if description:
            item_loader.add_value("description",description)
        furnished=response.xpath("//li[contains(.,'arredato')]").get()
        if furnished:
            item_loader.add_value("furnished",True)

        energy_label=response.xpath("//p[@class='h2 text-right font-weight-bold']//text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)

        images = [response.urljoin(x)for x in response.xpath("//a[@data-fancybox='group']//img//@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_phone", "06/57300244, 06/57300217")
        item_loader.add_value("landlord_email", "immobiliare.volpesrl@gmail.com ")
        item_loader.add_value("landlord_name", "IMMOBILIARE VOLPE")

        yield item_loader.load_item()