# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from parsel.utils import extract_regex
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'immocd_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Immocd_PySpider_belgium'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://immocd.be/Rechercher/Appartement%20Locations%20/Locations/Type-03%7CAppartement/Localisation-/Prix-/Tri-PRIX%20ASC,COMM%20ASC,CODE", "property_type": "apartment"},
	        {"url": "https://immocd.be/Rechercher/Maison%20Locations%20/Locations/Type-01%7CMaison/Localisation-/Prix-/Tri-PRIX%20ASC,COMM%20ASC,CODE", "property_type": "house"},

        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='zoom-cont2 hvr-grow']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={'property_type': response.meta.get('property_type')})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type",response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//h1[@class='liste-title']/span/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=adres
        if city:
            item_loader.add_value("city",city.split(" ")[0])
        zipcode=adres
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(" ")[-1])
        description=response.xpath("//div[@class='col-md-6']/p/text()").getall()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//div[@class='thumb hvr-wobble-vertical']/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        rent=response.xpath("//td[.='Price']/following-sibling::td/text()").get()
        if rent and not "Price" in rent:
            item_loader.add_value("rent",rent.split("€")[0].strip().replace("\xa0",""))
        item_loader.add_value("currency","EUR")
        utilities=response.xpath("//td[.='Rental loads']/following-sibling::td/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].strip())
        furnished=response.xpath("//td[.='Furnished']/following-sibling::td/text()").get()
        if furnished and furnished=='Yes': 
            item_loader.add_value("furnished",True)
        available_date=response.xpath("//td[.='Availability']/following-sibling::td/text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)
        square_meters=response.xpath("//td[.='Net living area']/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].strip())
        room_count=response.xpath("//td[.='Bedrooms']/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        floor=response.xpath("//td[.='étage']/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        available_date=response.xpath("//td[.='Availability']/following-sibling::td/text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)

        yield item_loader.load_item()