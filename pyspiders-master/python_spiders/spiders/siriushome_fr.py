# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import math
class MySpider(Spider):
    name = 'siriushome_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Siriushome_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://siriushome.fr/?s=&es_search%5Bes_status%5D%5B%5D=152&es_search%5Bes_type%5D%5B%5D=149&es_search%5Bville1624438606f60d2f74eac14a%5D=&post_type=properties",
                "property_type" : "apartment"
            },      
            {
                "url" : "https://siriushome.fr/?s=&es_search%5Bes_status%5D%5B%5D=152&es_search%5Bes_type%5D%5B%5D=147&es_search%5Bville1624438606f60d2f74eac14a%5D=&post_type=properties",
                "property_type" : "house"
            },      
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='ert-property-item__image-inner']/a/@href").getall():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )     
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//h2[@class='entry-title']/text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//span[@class='es-price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].replace(".",""))
        item_loader.add_value("currency","GBP")
        room_count=response.xpath("//span[.='Chambres: ']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[.='Salles de bain: ']/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        square_meters=response.xpath("//span[.='Superficie: ']/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].strip())
        description="".join(response.xpath("//h3[.='Description']/following-sibling::p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        latitude=response.xpath("//a[@class='es-map-view-link es-hover-show']/@data-latitude").extract()
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//a[@class='es-map-view-link es-hover-show']/@data-longitude").extract()
        if longitude:
            item_loader.add_value("longitude",longitude)
        name=response.xpath("//h4[@class='ert-agent__name']/span/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        email=response.xpath("//ul[@class='ert-agent__fields']/li/b/text()").get()
        if email:
            item_loader.add_value("landlord_email",email)
        yield item_loader.load_item()