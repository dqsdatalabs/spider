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
    name = 'agenceduchateausaintnom_com'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="Agenceduchateausaintnom_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://arcanaimmobilier.fr/annonces/louer/",
                ],
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='wpsight-listing-image']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item,)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//h1[@class='entry-title']/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=title
        if "appartement" in property_type.lower():
            item_loader.add_value("property_type","apartment")
        if "maison" in property_type.lower():
            item_loader.add_value("property_type","house")
        if "entrepot" in property_type.lower():
            return 
        dontallow=response.url
        if "local" in dontallow:
            return 
        adres=response.url
        if adres:
            adres=adres.split("maison-")[-1].split("appartement-")[-1].split("piece")[0].split("-")[:-1]
            if adres:
                item_loader.add_value("address",adres)
        rent=response.xpath("//span[@class='listing-price-value']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace(".",""))
        item_loader.add_value("currency","EUR")
        square_meters=response.xpath("//span[.='Superficie (m²)']/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[0].strip())
        room_count=response.xpath("//span[.='Pièces']/following-sibling::span/text() | //span[.='Chambres']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[.='Salles de bain']/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        descripiton=response.xpath("//div[@class='wpsight-listing-description']/p/text()").get()
        if descripiton:
            item_loader.add_value("description",descripiton)
        images=[x for x in response.xpath("//img[@itemprop='thumbnail']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        terrace=response.xpath("//span[.='Terrain (m²)']").get()
        if terrace:
            item_loader.add_value("terrace",True)
        item_loader.add_value("landlord_name","Arcana Immobilier")

            
        yield item_loader.load_item()