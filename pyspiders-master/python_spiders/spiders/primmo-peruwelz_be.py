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
    name = 'primmo-peruwelz_be' 
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='PrimmoPeruwelz_PySpider_belgium'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.primmo.be/index.php?page=0&action=list&ctypmandatmeta=l&ctypmeta=appt&llocalite=&mprixmin=&mprixmax=&cbien=#toplist",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.primmo.be/index.php?action=list&ctypmandatmeta=l&ctypmeta=mai&llocalite=&mprixmin=&mprixmax=&cbien=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url["url"]:
                yield Request(item,callback=self.parse,meta={'property_type': url['property_type']})
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//article/figure/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        nextpage=response.xpath("//a[.='Suivant >']/@href").get()
        if nextpage:
            yield Request(response.urljoin(nextpage),callback=self.parse, meta={"property_type":response.meta["property_type"]})
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        title=response.xpath("//h3[@class='headline']/text()").get()
        if title:
            item_loader.add_value("title",title)
        description=response.xpath("//div[@id='desc']/p/text()").get()
        if description:
            item_loader.add_value("description",description)
        item_loader.add_value("address","PÉRUWELZ")
        dontallow=response.xpath("//div[@class='sixteen columns ']/h2/text()").get()
        if dontallow and "commerce" in dontallow.lower():
            return 
        rent=response.xpath("//li[contains(.,'Prix')]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split(":")[-1].split("€")[0].strip())
        item_loader.add_value("currency","EUR")
        room_count=response.xpath("//li[contains(.,'Chambre')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        bathroom_count=response.xpath("//li[contains(.,'Salle de bains')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip().split(" ")[0])
        square_meters=response.xpath("//li[contains(.,'Surface habitable')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[-1].split("m²")[0].strip())
        utilities=response.xpath("//li[contains(.,'Charges')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0].strip())
        external_id=response.xpath("//p[contains(.,'Réf.:')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        images=[x for x in response.xpath("//div/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        terrace=response.xpath("//li[contains(.,'Terrasse:')]").get()
        if terrace:
            item_loader.add_value("terrace",True)
        energy_label=response.xpath("//strong[.='Consommation énergétique:']/following-sibling::img/following-sibling::text()").get()
        if energy_label:
            energy = energy_label.split("k")[0].replace("\r\n","").strip()
            item_loader.add_value("energy_label",energy_label_calculate(int(float(energy.replace(",",".")))))
        name=response.xpath("//div[@class='content-box color-effect-1']/p/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//div[@class='content-box color-effect-1']/p//a[contains(@href,'tel')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        email=response.xpath("//div[@class='content-box color-effect-1']/p//a[contains(@href,'mailto')]/text()").get()
        if email:
            item_loader.add_value("landlord_email",email)
        

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