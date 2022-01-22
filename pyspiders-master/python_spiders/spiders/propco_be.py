# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from typing import NewType
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
    name = 'propco_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Propco_PySpider_belgium'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "http://propco.be/index.php?ctypmandatmeta=l&action=list&ctypmandatl=1&ctypmandatlm=1&ctypmandatls=1&cbien=&ctypmeta=&qchambres=&cregion=&llocalite=&mprixmax=&cisopays=BEL"}
        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 1)
        seen = False
        for item in response.xpath("//div[@class='picture']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen=True
        if page == 1 or seen:
            nextpage=f"http://propco.be/index.php?page={page}&ctypmandatmeta=l&action=list&ctypmandatl=1&ctypmandatlm=1&ctypmandatls=1&cbien=&ctypmeta=&qchambres=&cregion=&llocalite=&mprixmax=&cisopays=BEL#toplist"
            if nextpage:
                yield Request(response.urljoin(nextpage), callback=self.parse,meta={'page':page+1})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//div[@class='headline']/h2/text()").get()
        if title:
            item_loader.add_value("title",title)
        description=response.xpath("//div[@class='headline']/following-sibling::p/text()").get()
        if description:
            item_loader.add_value("description",description)
        external_id=response.xpath("//p[contains(.,'Réf.')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        images=[x for x in response.xpath("//li//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        room_count=response.xpath("//li[contains(.,'Chambres')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        bathroom_count=response.xpath("//li[contains(.,'Salle de bains')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip().split(" ")[0])
        square_meters=response.xpath("//li[contains(.,'Surface habitable:')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[-1].split("m")[0])
        utilities=response.xpath("//li[contains(.,'Charges:')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0])
        terrace=response.xpath("//li[contains(.,'Terrasse')]/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        rent=response.xpath("//b[contains(.,'Prix')]/text()").get()
        if rent and not "Loué" in rent:
            item_loader.add_value("rent",rent.split(":")[-1].split("€")[0].strip())
        item_loader.add_value("currency","EUR")
        elevator=response.xpath("//li[.='Ascenseur ']/text()").get()
        if elevator:
            item_loader.add_value("elevator",True)
        energy_label=response.xpath("//strong[.='Consommation énergétique:']/parent::p/img/following-sibling::text()").get()
        if energy_label:
            energy = energy_label.replace("(","").replace(")","").split("k")[0]
            item_loader.add_value("energy_label",energy_label_calculate(int(float(energy.replace(",",".")))))
        item_loader.add_value("city","Bruxelles")
        item_loader.add_value("landlord_email","contact@propco.be")
        item_loader.add_value("landlord_phone","+32 473 475 107")
        item_loader.add_value("landlord_name","PROPCO")
            



        yield item_loader.load_item() 



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