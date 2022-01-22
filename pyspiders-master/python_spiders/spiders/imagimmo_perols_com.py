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

class MySpider(Spider):
    name = "imagimmo_perols_com" # LEVEL 1
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = 'Imagimmo_Perols_PySpider_france' 
    
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.imagimmo-perols.com/biens/recherche/louer/toutes-villes/maison/tous-prix/toutes-surfaces/toutes-tailles",
                "property_type": "house"
            },
            {
                "url": "https://www.imagimmo-perols.com/biens/recherche/louer/toutes-villes/appartement/tous-prix/toutes-surfaces/toutes-tailles",
                "property_type": "apartment"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='info']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = response.xpath("//h1[@class='title']/text()").get()
        if title:
            item_loader.add_value("title", title)

        external_id=response.xpath("//h2/text()[contains(.,'Référence')]").get()
        if external_id:
            external_id = re.findall(r'\d+', external_id)
            item_loader.add_value("external_id",external_id)

        rent=response.xpath("//h1[@class='title']/span/text()[contains(.,'€')]").get()
        if rent:
            item_loader.add_value("rent",rent.split('€')[0].replace(" ","").strip())
        item_loader.add_value("currency","EUR")

        square_meters=response.xpath("//li[contains(.,'habitable')]/div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].strip())
        
        room_count=response.xpath("//li[contains(.,'pièces')]/div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(" ")[0].strip())
        
        bathroom_count=response.xpath("//li[contains(.,'salle')]/div/text()").get()
        if bathroom_count:
            bathroom_count = re.findall(r'\d+', bathroom_count)
            item_loader.add_value("bathroom_count",bathroom_count)

        floor=response.xpath("//li[contains(.,'étage')]/div/text()").get()
        if floor:
            floor = re.findall(r'\d+', floor)
            if floor:
                item_loader.add_value("floor",floor)
        
        energy_label=response.xpath("(//li[contains(@class,'selected')])[1]/span[@class='lettre']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)

        parking = response.xpath("//li[contains(.,'garage')]/div/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//li[contains(.,'parking')]/div/text()").get()
            if parking:
                item_loader.add_value("parking", True)

        parking = response.xpath("//li[contains(.,'garage')]/div/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        desc=response.xpath("//p[@class='description']/text()").getall()
        if desc:
            item_loader.add_value("description",desc)

        latlng = response.xpath("//script[@type='text/javascript'][contains(.,'deltas')]/text()").get()
        if latlng:
            latitude = latlng.split('"lat":"')[-1].split('",')[0]
            item_loader.add_value("latitude", latitude)
            longitude = latlng.split('"lng":"')[-1].split('"}')[0]
            item_loader.add_value("longitude", longitude)

        images = [x for x in response.xpath("//div[contains(@class,'galleria')]/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        item_loader.add_value("landlord_name", "IMAGIMMO PEROLS")
        item_loader.add_value("landlord_phone", "04 67 50 07 78")
        
        yield item_loader.load_item()