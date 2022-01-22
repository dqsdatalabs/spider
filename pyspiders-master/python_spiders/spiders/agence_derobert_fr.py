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
    name = "agence_derobert_fr" # LEVEL 1
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = 'Agencederobert_PySpider_france' 
    def start_requests(self):
        start_urls = [
            {"url": "https://www.agence-derobert.fr/en/listing.html?loc=location&type%5B%5D=maison&surfacemin=&prixmax=&tri=prix-asc&page=1&coordonnees=&supplementaires=0&prixmin=&surfacemax=&terrain=&numero=&idpers=&options=&telhab=&piecemin=", "property_type": "house"},
            {"url": "https://www.agence-derobert.fr/en/listing.html?loc=location&type%5B%5D=appartement&surfacemin=&surfacemax=&prixmin=&prixmax=&piecemin=&chambremin=&terrainmin=&numero=&tri=&page=1&btnSubmit=Search", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//article[@class='item-listing']//a//@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,
                    meta={'property_type': response.meta.get('property_type')})

    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title","//title//text()")
        external_id=response.xpath("//span[contains(.,'Ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1])

        rent=response.xpath("//script[contains(.,'price')]/text()").get()
        if rent:
            rent=rent.split('price":')[-1].split(",")[0]
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        square_meters=response.xpath("//ul[@class='card_list']//li//sup/parent::li/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[0])
        room_count=response.xpath("//ul[@class='card_list']//li//text()[contains(.,'rooms')]/parent::li/span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[contains(.,'Shower room')]/parent::span/following-sibling::span/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        city=response.xpath("//span[contains(.,'Town')]/parent::span/following-sibling::span/span/text()").get()
        if city:
            item_loader.add_value("city",city.replace("\n","").strip().capitalize())
        floor=response.xpath("//span[contains(.,'Floor')]/parent::span/following-sibling::span/span/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        latitude=response.xpath("//script[contains(.,'centerLngLat')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("lat:")[-1].split(",")[0].strip())
        longitude=response.xpath("//script[contains(.,'centerLngLat')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("lng:")[-1].split("}")[0].replace("\n","").strip())

        desc=response.xpath("//h2[@class='info_titre mb-3']/following-sibling::text()").getall()
        if desc:
            item_loader.add_value("description",desc)
        images=response.xpath("//div[@id='carouselImages']//img//@src").getall()
        if images:
            item_loader.add_value("images",images)
        

        item_loader.add_value("landlord_name","Agence Immobiliare Derobert")
        item_loader.add_value("landlord_phone","01 30 62 31 62")

        yield item_loader.load_item()