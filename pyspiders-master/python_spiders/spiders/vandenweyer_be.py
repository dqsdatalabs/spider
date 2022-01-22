# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

# from tkinter.font import ROMAN
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader 
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'vandenweyer_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Vandenweyer_PySpider_belgium'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://vandenweyer.be/nl/te-huur/?type%5B%5D=1&price-min=&price-max=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://vandenweyer.be/nl/te-huur/?type%5B%5D=5&price-min=&price-max=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url["url"]:
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url['property_type']})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='spotlight__image ']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title=response.xpath("//div[@class='col-md-8']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//div[@class='property__header-block__adress__street']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        external_id=response.xpath("//div[@class='property__header-block__ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        description=response.xpath("//div[@class='property__details__block__description']/text()").get()
        if description:
            item_loader.add_value("description",description)
        rent=response.xpath("//td[.='Prijs']/following-sibling::td/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].split(",")[0].strip())
        item_loader.add_value("currency","EUR")
        available_date=response.xpath("//td[.='Vrij op']/following-sibling::td/text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)
        parking=response.xpath("//td[.='Aantal garages']/following-sibling::td/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        energy_label=response.xpath("//td[.='EPC waarde']/following-sibling::td/text()").get()
        if energy_label:
            energy = energy_label.split("k")[0].strip()
            item_loader.add_value("energy_label",energy_label_calculate(int(float(energy.replace(",",".")))))
        images=[x for x in response.xpath("//a[@class='picture-lightbox']/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        latitude=response.xpath("//div[@id='pand-map']/@data-geolat").get()
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//div[@id='pand-map']/@data-geolong").get()
        if longitude:
            item_loader.add_value("longitude",longitude)
        item_loader.add_value("landlord_name","Zakenkantoor Vandenweyer bvba")
        room_count=response.xpath("//td[.='Aantal slaapkamers']/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)



        yield item_loader.load_item() 

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label