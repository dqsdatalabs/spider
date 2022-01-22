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

class MySpider(Spider):
    name = 'avegimmo_com'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="Avegimmo_PySpider_france"
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.avegimmo.com/location/1",
                ],

            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,callback=self.parse,)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@itemprop='url']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item,)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//h1[@class='titleBien']/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.url
        if property_type and "maison" in property_type.lower():
            item_loader.add_value("property_type","house")
        if property_type and "appartement" in property_type.lower():
            item_loader.add_value("property_type","apartment")
        adres=response.url
        if adres:
            adres=adres.split("93-seine-saint-denis/")[-1].split("/")[0]
            item_loader.add_value("address",adres.replace("-"," ").title())
        zipcode=response.xpath("//li[contains(.,'Code postal')]/span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        square_meters=response.xpath("//li[contains(.,'Surface habitable (m²)')]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].split(",")[0])
        room_count=response.xpath("//li[contains(.,'Nombre de pièces')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        furnished=response.xpath("//li[contains(.,'Meublé')]/span/text()").get()
        if furnished and furnished=="NON":
            item_loader.add_value("furnished",False)
        bathroom_count=response.xpath("//li[contains(.,'Nb de salle d')]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        parking=response.xpath("//li[contains(.,'Nombre de parking')]/span/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        description=response.xpath("//div[@class='offreContent bxd6 bxt12']/p/text()").get()
        if description:
            item_loader.add_value("description",description)
        rent=response.xpath("//p[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip())
        item_loader.add_value("currency","EUR")
        external_id=response.xpath("//p[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1])
        images=[x for x in response.xpath("//li//picture//img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        utilities=response.xpath("//li[contains(.,'Honoraires TTC charge locataire')]/span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].split(",")[0])
        deposit=response.xpath("//li[contains(.,'Honoraires TTC charge locataire')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0].split(",")[0])

        yield item_loader.load_item()