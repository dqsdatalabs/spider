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
    name = 'as-immo-28_com'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="Asimmo28_PySpider_france"
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
                    "https://www.as-immo-28.com/location/appartements/1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.as-immo-28.com/location/maisons-villas/1",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//button[@class='btnListingDefault btnBoldListing obfusquer']/@data-url").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//p[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip())
        item_loader.add_value("currency","EUR")
        zipcode=response.xpath("//li[contains(.,'Code postal')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(":")[-1].strip())
        square_meters=response.xpath("//li[contains(.,'Surface habitable (m²)')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[-1].split(",")[0].split("m²")[0])
        room_count=response.xpath("//li[contains(.,'Nombre de pièces')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(":")[-1].strip())
        furnished=response.xpath("//li[contains(.,'Meublé')]/text()").get()
        if furnished:
            if "NON" in furnished:
                item_loader.add_value("furnished",False)
            if "OUI" in furnished:
                item_loader.add_value("furnished",True)
        elevator=response.xpath("//li[contains(.,'Ascenseur')]/text()").get()
        if elevator:
            if "NON" in elevator:
                item_loader.add_value("elevator",False)
            if "OUI" in elevator:
                item_loader.add_value("elevator",True)
        bathroom_count=response.xpath("//li[contains(.,'Nb de salle de bains')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(":")[-1])
        balcony=response.xpath("//li[contains(.,'Balcon')]/text()").get()
        if balcony:
            if "NON" in balcony:
                item_loader.add_value("balcony",False)
            if "OUI" in balcony:
                item_loader.add_value("balcony",True)
        terrace=response.xpath("//li[contains(.,'Terrasse')]/text()").get()
        if terrace:
            if "NON" in terrace:
                item_loader.add_value("terrace",False)
            if "OUI" in terrace:
                item_loader.add_value("terrace",True)
        parking=response.xpath("//li[contains(.,'Nombre de parking')]/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        utilities=response.xpath("//li[contains(.,'Honoraires TTC charge locataire')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0].split(",")[0].strip())
        deposit=response.xpath("//li[contains(.,'Dépôt de garantie TTC')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split("€")[0].split(",")[0].strip())
        description=response.xpath("//h2[@class='titleDetail']/following-sibling::p/text()").get()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//picture/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        external_id=response.xpath("//p[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        item_loader.add_value("landlord_name","ASIMMO")


        yield item_loader.load_item()