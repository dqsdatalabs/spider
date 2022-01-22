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
    name = 'ageco_net'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="Ageco_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.ageco.net/fr/liste.htm?menu=2&page=1#menuSave=2&page=1&ListeViewBienForm=text&ope=2&filtre2=2",
                ],
                "property_type" : "apartment",
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='liste-bien-photo mode-2']/div/a/@href").getall():
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
        description=response.xpath("//span[@itemprop='description']/text()").get()
        if description:
            item_loader.add_value("description",description)
        external_id=response.xpath("//span[.='Ref']/following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        rent=response.xpath("//li//text()[contains(.,'par mois')]").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip().replace(" ",""))
        item_loader.add_value("currency","EUR")
        type=response.xpath("//span[.='Type']/following-sibling::text()").get()
        if type and ("Parking" in type or "Local" in type):
            return 
        city=response.xpath("//span[.='Ville']/following-sibling::text()").get()
        if city:
            item_loader.add_value("city",city)
        square_meters=response.xpath("//span[.='Surface']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        deposit=response.xpath("//span[contains(.,'Dépôt')]/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0].replace(" ",""))
        utilities=response.xpath("//span[contains(.,'Honoraires')]/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].split(".")[0])
        images=[x for x in response.xpath("//img[@class='photo-slideshow photo-thumbs']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        room_count=response.xpath("//span[.='Pièces']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        adres=response.xpath("//span[.='Ville']/following-sibling::text()").get()
        if adres:
            item_loader.add_value("address",adres.strip())
        item_loader.add_value("landlord_name","AGECO")
        item_loader.add_value("landlord_phone","09 82 59 06 89")
            

        yield item_loader.load_item()