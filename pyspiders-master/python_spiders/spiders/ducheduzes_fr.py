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
    name = 'ducheduzes_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Ducheduzes_PySpider_france"
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    } 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.ducheduzes.fr/fr/annonces/location-p-r70-2-1.html#menuSave=2&page=1&TypeModeListeForm=text&vlc=2",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='liste-bien-photo']/div/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//title/text()").get()
        item_loader.add_value("title", re.sub('\s{2,}', ' ',title.strip()))
        property_type=response.xpath("//h1[@class='detail-bien-type']/span/text()").get()
        if property_type:
            if "Villa"==property_type:
                item_loader.add_value("property_type","house")
            if "Appartement"==property_type:
                item_loader.add_value("property_type","apartment")
        square_meters=response.xpath("//svg[@class='icon icon-surface']/parent::span/parent::li/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        room_count=response.xpath("//svg[@class='icon icon-room']/parent::span/parent::li/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("pi")[0].strip())
        rent=response.xpath("//div[@class='detail-bien-prix']/span/following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip().replace(" ",""))
        item_loader.add_value("currency","EUR")
        adres=response.xpath("//h2[@class='detail-bien-ville']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        zipcode=response.xpath("//h2[@class='detail-bien-ville']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split("(")[1].split(")")[0])
        external_id=response.xpath("//span[@itemprop='productID']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        description=response.xpath("//p[@itemprop='description']/text()").get()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//img[@class='photo-slideshow photo-thumbs']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        utilities=response.xpath("//li[contains(.,'Honoraires charge locataire')]/span[2]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities)
        deposit=response.xpath("//li[contains(.,'Dépôt de garantie')]/span[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit)
        item_loader.add_value("landlord_name","Duche Duzes")
        
       
        
        yield item_loader.load_item()
        

