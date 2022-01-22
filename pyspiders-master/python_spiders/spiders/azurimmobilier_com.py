# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'azurimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Azurimmobilier_PySpider_france"
    custom_settings = {'HTTPCACHE_ENABLED': False}
    def start_requests(self):
        
        start_urls = [
            {   "category":"Location|2",
                "property_type" : "apartment",
                "type" : "Appartement|1"
            },

            {   "category":"Location|2",
                "property_type" : "house",
                "type" : "Maison|2"
            },

        ]

        for item in start_urls:
      
            formdata = {
                "search-form-98186[search][category]":  item.get("category"),
                "search-form-98186[search][type]": item.get("type"),
                "search-form-98186[search][city]": "",
                "search-form-98186[search][price_min]": "",
                "search-form-98186[search][price_max]": "",
                "search-form-98186[submit]": "",
                "search-form-98186[search][order]": "",
            }
            yield FormRequest(
                "https://azurimmobilier.com/fr/recherche",
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":item["property_type"]
                }
 
            )
    # 1. FOLLOWING
    def parse(self, response): 

        for item in response.xpath("//ul[@class='_list listing']//li[@class='property initial']/@id").getall():
            url=f"https://azurimmobilier.com/fr/propri%C3%A9t%C3%A9/{item}"
            yield Request(url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Azurimmobilier_PySpider_france")
        rent=response.xpath("//p[contains(.,'Mois')]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].replace(" ",""))
        item_loader.add_value("currency","EUR")
        title="".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title",title)
        external_id=response.xpath("//p[contains(.,'Réf')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(".")[-1])
        room_count=response.xpath("//li[contains(.,'Pièces')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(" ")[0])
        floor=response.xpath("//li[contains(.,'Étage')]/span/text()").get()
        if floor:
            item_loader.add_value("floor",floor.split("er")[0])
        description=response.xpath("//p[@id='description']/text()").get()
        if description:
            item_loader.add_value("description",description)
        square_meters=response.xpath("//div[@class='module module-98172 module-property-info property-info-template-18 ']//p[contains(.,'m²')]//text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0])
        images=response.xpath("//div[@class='item']/img/@data-src").getall()
        if images:
            item_loader.add_value("images",images)
        deposit=response.xpath("//li[contains(.,'Dépôt de garantie')]/text()/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0])


        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Agence Azur Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 42 56 56 36", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="gilbert.galban@hotmail.fr", input_type="VALUE")

        yield item_loader.load_item()