# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

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
    name = 'immo-vandenabeele'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Immovandenabeele_PySpider_belgium'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agencevandenabeele.be/nl/api/properties.json?grid=1240,1599,10012&type=appartement&pg=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agencevandenabeele.be/nl/api/properties.json?grid=1240,1599,10012&type=woning&pg=1",
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

        data=json.loads(response.body)['data']
        for item in data:
            yield Request(item['url'], callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        title=response.xpath("//h1[@class='s-title s-title--default']/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//a[contains(@href,'maps/search')]/text()").get()
        if adres:
            item_loader.add_value("address",adres.replace(" ","").replace("\xa0"," "))
        rent=response.xpath("//div[@class='s-text--large s-color--red']/strong/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].strip().replace(" ",""))
        item_loader.add_value("currency","EUR")
        images=[x for x in response.xpath("//a[@data-type='image']/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        room_count=response.xpath("//div[.='Slaapkamers']/following-sibling::div/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        square_meters=response.xpath("//div[.='Bewoonbare opp.']/following-sibling::div/strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0])
        terrace=response.xpath("//div[.='(Stads)tuin']/following-sibling::div/strong/text()").get()
        if terrace and terrace=="Ja":
            item_loader.add_value("terrace",True)
        parking=response.xpath("//div[.='Garage']/following-sibling::div/strong/text()").get()
        if parking:
            item_loader.add_value("parking",True)


        energy_label=response.xpath("//div[.='EPC']/following-sibling::div/strong/text()").get()
        if energy_label:
            energy = energy_label.split("k")[0].replace("\r\n","").split(".")[0].strip()
            item_loader.add_value("energy_label",energy_label_calculate(int(float(energy.replace(",",".")))))
        dontallow=response.xpath("//div[@class='s-color--red s-bold']/strong/text()").get()
        if dontallow and dontallow=="Verhuurd":
            return 
        item_loader.add_value("landlord_name","Agence Van Den Abeele")
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