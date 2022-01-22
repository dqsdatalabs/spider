# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'billet-giraud_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="BilletGiraud_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.billet-giraud.fr/type_bien/3-32/a-louer.html",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.billet-giraud.fr/type_bien/4-39/a-louer.html",
                ],
                "property_type" : "house"
            },


        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='product-listing']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        nextpage=response.xpath("//li[@class='next-link active']/a/@href").get()
        if nextpage:
            yield Request(
            response.urljoin(nextpage),
            callback=self.parse, meta={"property_type":response.meta["property_type"]}
        )


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link",response.url)
        item_loader.add_value("external_source",self.external_source)
        title=response.xpath("//div[@class='infos-products-header']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//div[.='Type de bien']/following-sibling::div/b/text()").get()
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        rent=response.xpath("//span[@class='alur_loyer_price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].split("Loyer")[1].replace("\xa0","").strip())
        item_loader.add_value("currency","GBP")
        external_id=response.xpath("//div[@class='product-ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[1])
        room_count=response.xpath("//ul[@class='list-criteres']//div[contains(.,'pièce(s)')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(" ")[0])
        bathroom_count=response.xpath("//ul[@class='list-criteres']//div[contains(.,'salle(s) d')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(" ")[0])
        square_meters=response.xpath("//ul[@class='list-criteres']//div[contains(.,'m²')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].split(".")[0])
        adres=response.xpath("//div[@class='product-localisation']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
            item_loader.add_value("city",adres.split(" ")[1])
            item_loader.add_value("zipcode",adres.split(" ")[0])
        description=response.xpath("//div[@class='product-description']/text()").get()
        if description:
            item_loader.add_value("description",description)
        elevator=response.xpath("//div[.='Ascenseur']/following-sibling::div/b/text()").get()
        if elevator and "oui" in elevator.lower():
            item_loader.add_value("elevator",True)

        images=[x for x in response.xpath("//div[@class='item-slider']/a/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "maison" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None