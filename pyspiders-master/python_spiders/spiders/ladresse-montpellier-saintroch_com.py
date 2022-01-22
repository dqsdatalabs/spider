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
import dateparser


class MySpider(Spider):
    name = 'ladresse-montpellier-saintroch_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Ladresse_Montpellier_Saintroch_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.ladresse-montpellier-saintroch.com/annonces/transaction/Location.html",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='products-img']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)


    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)

        item_loader.add_value("external_link", response.url)


        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)

        
        if title:
            if "villa" in title.lower():
                item_loader.add_value("property_type","house")
            if "appartement" in title.lower():
                item_loader.add_value("property_type","apartment")

        id = response.xpath("//span[contains(text(),'Ref.')]/text()").get()
        if id:
            id = id.split(":")[-1].strip()
            item_loader.add_value("external_id",id)

        images = [response.urljoin(img) for img in response.xpath("//li/a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)

        rent = response.xpath("//span[@class='alur_loyer_price']/text()").get()
        if rent:
            rent = re.search("[\d]+",rent)[0]
            item_loader.add_value("rent",rent)

        room = response.xpath("//span[contains(text(),'ièces')]/text()").get()
        if room:
            room = room.split()[0]
            item_loader.add_value("room_count",room)

        surface = response.xpath("//span[contains(text(),'m²')]/text()").get()
        if surface:
            surface = surface.split(".")[0]
            item_loader.add_value("square_meters",surface)

        bath = response.xpath("//img[contains(@src,'picto-bain')]/following-sibling::span/text()").get()
        if bath:
            item_loader.add_value("bathroom_count",bath)

        floor = response.xpath("//img[contains(@src,'picto-etage')]/following-sibling::span/text()").get()
        if floor:
            floor = floor.split("/")[0]
            item_loader.add_value("floor",floor)

        desc = " ".join(response.xpath("//div[@class='content-desc']/text()").getall())
        if desc:
            item_loader.add_value("description",desc)

        address = response.xpath("//span[@class='alur_location_ville']/text()").get()
        if address:
            zipcode = address.split()[0]
            city = address.split()[-1]
            item_loader.add_value("zipcode",zipcode)
            item_loader.add_value("city",city)  
            item_loader.add_value("address",address)  

        item_loader.add_value("currency","EUR")
        item_loader.add_value("landlord_name"," l'Adresse real estate")
        item_loader.add_value("landlord_phone","09.87.52.53.35")
        

        yield item_loader.load_item()        