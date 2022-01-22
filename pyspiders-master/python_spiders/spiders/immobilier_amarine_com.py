# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import math

class MySpider(Spider):
    name = 'immobilier_amarine_com' # LEVEL 1
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = 'Immobilier_Amarine_PySpider_france' 
    
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.immobilier-amarine.com/fr/annonces/locations-sete-p-r70-2-1.html",
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[contains(.,'détails')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        title = response.xpath("(//title//text())[1]").get()
        if title:
            item_loader.add_value("title", title)
            if title and "studio" in title.lower():
                item_loader.add_value("property_type", "studio")
            elif title and "parking" in title.lower():
                return
            elif title and "box" in title.lower():
                return
            elif title and "bureau" in title.lower():
                return
            else:
                item_loader.add_value("property_type", "apartment")

        external_id=response.xpath("//div[@class='detail-bien-ref no-border-t']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.strip())

        rent=response.xpath("//div[@class='detail-bien-prix']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split('€')[0].replace(" ","").strip())
        item_loader.add_value("currency","EUR")

        square_meters=response.xpath("//span[@class='ico-surface']/parent::li/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.replace(" ","").split("m")[0].strip())
        
        room_count=response.xpath("//span[@class='ico-piece']/parent::li/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.replace(" ","").split("pièce")[0].strip())
        
        deposit=response.xpath("//span[@class='charges_mens'][contains(.,'Dépôt')]/following-sibling::span/text()").get()
        if (float(deposit) > 0):
            item_loader.add_value("deposit",deposit)

        uti=response.xpath("//span[@class='charges_mens'][contains(.,'Honoraires ')]/following-sibling::span/text()").get()
        if uti:
            utilities = math.ceil(float(uti))
            if utilities:
                item_loader.add_value("utilities",utilities)

        address = response.xpath("//h2[@class='detail-bien-ville']/text()").get()
        if address:
            item_loader.add_value("address", address)
            if address and "(" in address:
                zipcode = address.split("(")[-1].split(")")[0].strip()
                item_loader.add_value("zipcode", zipcode)
            city = address.split(" ")[0]
            if city:
                item_loader.add_value("city", city)

        desc=response.xpath("//div[@class='detail-bien-desc-content clearfix']/p/text()").getall()
        if desc:
            item_loader.add_value("description",desc)

        images = [x for x in response.xpath("//div[@class='diapo is-flap']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        item_loader.add_value("landlord_name", "Agence AMARINE")
        item_loader.add_value("landlord_phone", "04.67.53.53.40")
        yield item_loader.load_item()