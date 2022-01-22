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
    name = 'agence-imo_com'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="Agenceimo_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.agence-imo.com/immobilier/pays/locations/france.htm",
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

        for item in response.xpath("//div[@class='span9']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title="".join(response.xpath("//h1[@itemprop='name']/text()").getall())
        if title:
            item_loader.add_value("title",title)
        if "parking" in title:
            return 
        adres=response.xpath("//h1[@itemprop='name']/text()[last()]").get()
        if adres:
            item_loader.add_value("address",adres)
        city=adres.split("(")[0]
        if city:
            item_loader.add_value("city",city)
        zipcode=adres.split("(")[-1].split(")")[0]
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        rent=response.xpath("//span[@itemprop='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        room_count=response.xpath("//div[.='Pièces']/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//div[.='Salle de bain']/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        square_meters=response.xpath("//div[.='Surface']/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(",")[0])
        utilities=response.xpath("//li[contains(.,'Charges')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0].strip())
        description="".join(response.xpath("//p[@itemprop='description']/text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        external_id=response.xpath("//div[@class='bloc-detail-reference']/span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        item_loader.add_value("landlord_name","IMO")
        item_loader.add_value("landlord_phone","01 39 11 24 62")

        yield item_loader.load_item()