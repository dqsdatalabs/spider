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
    name = 'hdf-immobilier_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    start_urls = ["https://www.a7v-immobilier.com/location/1"]
    external_source='HdfImmobilier_PySpider_france'

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@itemprop='url']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        property_type=response.url
        if property_type and "maison" in property_type:
            item_loader.add_value("property_type","house")
        title=response.xpath("//h1[@class='titleBien']/text()").get()
        if title:
            item_loader.add_value("title",title)
        desc=response.xpath("//h2[@class='titleDetail']/following-sibling::p/text()").get()
        if desc:
            item_loader.add_value("description",desc)
        zipcode=response.xpath("//li[contains(.,'Code postal')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(":")[-1].strip())
        city=response.xpath("//li[contains(.,'Ville')]/text()").get()
        if city:
            item_loader.add_value("city",city.split(":")[-1])

        square_meters=response.xpath("//li[contains(.,'Surface habitable')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[-1].split("m")[0])
        room_count=response.xpath("//li[contains(.,'Nombre de pièces')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(":")[-1])
        floor=response.xpath("//li[contains(.,'Etage')]/text()").get()
        if floor:
            item_loader.add_value("floor",floor.split(":")[-1])
        externalid=response.xpath("//p[@class='ref']/text()").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split(":")[-1])
        rent=response.xpath("//p[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip())
        images=[x for x in response.xpath("//img[@class='img_Slider_Mdl']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        yield item_loader.load_item()