# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime
from datetime import date
import dateparser

class MySpider(Spider):
    name = 'leloupimmobilier_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    start_urls = ["https://www.leloupimmobilier.fr/location?input-secteur=&input-chx=Louer&input-budget=&input-secteur-m=Secteur&submit=Rechercher"]
    external_source='Leloupimmobilier_PySpider_france'

    # 1. FOLLOWING
    def parse(self, response):

        for item in  response.xpath("//ul[@class='listing-achat']//a//@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        rent=response.xpath("//p[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip())
        item_loader.add_value("currency","GBP")
        external_id=response.xpath("//p[contains(.,'Réf')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        title=response.xpath("//h2[@style='text-tranform:capitalize']/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//h2[@style='text-tranform:capitalize']/following-sibling::h3/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        zipcode=response.xpath("//h2[@style='text-tranform:capitalize']/following-sibling::h3/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip().split(" ")[0])
        city=response.xpath("//h2[@style='text-tranform:capitalize']/following-sibling::h3/text()").get()
        if city:
            item_loader.add_value("city",city.strip().split(" ")[1])
        deposit=response.xpath("//div[@class='Dépôt de garantie']/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split("€")[0])
        description=response.xpath("//div[@class='commentaire']/text()").get()
        if description:
            item_loader.add_value("description",description)
        square_meters=response.xpath("//div/sup[.='2']/preceding-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        images=[x for x in response.xpath("//div[@class='numbertext']/following-sibling::img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        room_count=response.xpath("//div[@class='details']//div[contains(.,'pièces')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        floor=response.xpath("//div[@class='details']//div[contains(.,'Etage')]/text()").get()
        if floor:
            item_loader.add_value("floor",floor.split(":")[-1])
        item_loader.add_value("landlord_phone","02 35 89 93 01")
        item_loader.add_value("landlord_name","LELOUP IMMOBILIER")

        yield item_loader.load_item()