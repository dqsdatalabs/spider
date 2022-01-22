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
    name = 'comprocasaf2_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Comprocasaf2_Pyspider_italy"

    # LEVEL 1
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://comprocasaf2.it/web/property-search/?status=affitto&type=residenziale",
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
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//article[@class='property-item clearfix']//figure/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"http://comprocasaf2.it/web/property-search/page/{page}/?status=affitto&type=residenziale"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title//text()")
        rent=response.xpath("//h5//span[2]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split(".")[0].replace("€","").replace(",","").strip())
        item_loader.add_value("currency","EUR")
        description=" ".join(response.xpath("//div[@class='im-description__text js-readAllText']/div/text() | //div[@class='col-xs-12 description-text text-expanded']/div/text() | //div[@class='im-description__text js-readAllText']/text() | //div[@id='tab-panel-1']/p/text() | //div[@class='content clearfix']/p/text()").getall())
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//span[contains(.,'Stanze da letto')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("Stanze")[0].strip())
        bathroom_count=response.xpath("//span[contains(.,'Bagni')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("Bagni")[0].strip())
        square_meters=response.xpath("//span[contains(.,'MQ')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("MQ")[0].strip())
        images=[x for x in response.xpath("//img//@src").getall()]
        if images:
            item_loader.add_value("images",images) 
        elevator=response.xpath("//h4[.='Caratteristiche']/following-sibling::ul//li[.='Ascensore']").get()
        if elevator:
            item_loader.add_value("elevator",True)
        furnished=response.xpath("//h4[.='Caratteristiche']/following-sibling::ul//li[.='Arredato']").get()
        if furnished:
            item_loader.add_value("furnished",True)
        balcony=response.xpath("//h4[.='Caratteristiche']/following-sibling::ul//li[.='Balcone']").get()
        if balcony:
            item_loader.add_value("balcony",True)
        item_loader.add_value("landlord_name","COMPROCASAf2")
        item_loader.add_value("landlord_phone","091 2744230")
        item_loader.add_value("landlord_email","info@comprocasaf2.it")

        item_loader.add_value("address","Palermo")
        item_loader.add_value("city","Palermo")

        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            external_id = external_id.split("=")[-1]
            item_loader.add_value("external_id",external_id)

        yield item_loader.load_item() 