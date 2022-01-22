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
    name = 'corumimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Corumimmobilier_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.corumimmobilier.com/annonces/location/montpellier-herault.html",
                ],
                "property_type":"apartment"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='voirannoncev2']/@onclick").getall():
            
            follow_url ="http://" + item.split("http://")[-1].strip("';")
            yield Request(follow_url, callback=self.populate_item)


    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)

        item_loader.add_value("external_link", response.url)

        title = response.xpath("//h1[@class='annonce']").get()
        if title:
            item_loader.add_value("title",title)


        rent = response.xpath("//div[@class='detail_the_prix']/text()").get()
        if rent:
            rent = rent.split("€")[0].strip()
            item_loader.add_value("rent",rent)

        id = response.xpath("//div[@id='detail_reference']/span/text()").get()
        if id:
            item_loader.add_value("external_id",id.strip())

        desc = response.xpath("//h3[@class='annonce']/text()").get()
        if desc:
            item_loader.add_value("description",desc)


        city = response.xpath("//span[text()='Ville :']/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("city",city)
            item_loader.add_value("address",city)

        zipcode = response.xpath("//span[text()='Code postal :']/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)

        square_meters = response.xpath("//span[text()='Surface habitable :']/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split()[0]
            item_loader.add_value("square_meters",square_meters)


        room_count = response.xpath("//span[text()='Nombre de pièces :']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)


        floor = response.xpath("//span[text()='Etage :']/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor",floor)


        utilities = response.xpath("//span[text()='Charges mensuelles :']/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities)


        floor = response.xpath("//span[text()='Etage :']/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor",floor)


        deposit = response.xpath("//span[text()='Dêpot de garantie :']/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit)

        bathroom_count = response.xpath("""//span[contains(text(),"Salle")]/following-sibling::span/text()""").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)


        images = response.xpath("//img[@class='petiteimage']/@src").getall()
        if images:
            item_loader.add_value("images",images)
        
        item_loader.add_value("currency","EUR")
        item_loader.add_value("landlord_name","CORUM IMMOBILIER")
        item_loader.add_value("landlord_phone","04 67 79 39 40")
        item_loader.add_value("property_type","apartment")
        item_loader.add_value("landlord_email","contact@corumimmobilier.com")



        yield item_loader.load_item()