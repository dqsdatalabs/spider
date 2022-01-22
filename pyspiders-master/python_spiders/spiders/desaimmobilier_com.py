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
    name = 'desaimmobilier_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = "Desaimmobilier_PySpider_france"
    start_urls = ['http://desaimmobilier.com/rechercher-bastia']  # LEVEL 1

    formdata = {
        "form[search_type]": "0",
        "form[min_chambres]": "",
        "form[min_surface]": "",
        "form[estate_type]": "",
        "form[max_price]": "",
        "form[_token]": ""
    }
    def start_requests(self):
        yield Request(
            url=self.start_urls[0],
            callback=self.get_post
        )

    def get_post(self, response):
        token = response.xpath("//input[@id='form__token']/@value").get()
        self.formdata["form[_token]"] = token
        start_urls = [
            {
                "url": [
                    "0",
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "1"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                self.formdata["form[estate_type]"] = item
                yield FormRequest(
                    url=self.start_urls[0],
                    callback=self.parse,
                    formdata=self.formdata,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[h3]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title//text()")

        rent=response.xpath("//h2[@itemprop='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0])
        item_loader.add_value("currency","EUR") 

        desc=" ".join(response.xpath("//span[@itemprop='description']/text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        square_meters=response.xpath("//ul//li[contains(.,'Surface')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[-1].split("m")[0])
        energy_label=response.xpath("//ul//li[contains(.,'DPE')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split(":")[-1])
        parking=response.xpath("//li[contains(.,'parking')]/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        terrace=response.xpath("//li[contains(.,'terrasse')]/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        floor=response.xpath("//ul//li[contains(.,'Etage')]/text()").get()
        if floor:
            item_loader.add_value("floor",floor.split(":")[-1])
        images=[response.urljoin(x) for x in response.xpath("//img[@itemprop='image']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","DESA IMMOBILIER")


        yield item_loader.load_item()