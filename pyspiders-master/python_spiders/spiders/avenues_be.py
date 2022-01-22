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
import dateparser  

class MySpider(Spider):
    name = "avenues_be" 
    execution_type = 'testing'
    country = 'belgium'
    locale='fr'
    external_source='Avenues_PySpider_belgium'
    def start_requests(self):
        start_urls = [
            {
                "url" : "http://www.avenues.be/index.php?action=list&ctypmandatmeta=l&ctypmeta=appt&llocalite=&mprixmin=&mprixmax=&cbien=",
                "property_type" : "apartment"
            },
            {
                "url" : "http://www.avenues.be/index.php?action=list&ctypmandatmeta=l&ctypmeta=mai&llocalite=&mprixmin=&mprixmax=&cbien=",
                "property_type" : "house"
            },
        ] # LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse, meta={'property_type': url.get('property_type')})
    def parse(self, response):
        for item in  response.xpath("//figure//a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type",response.meta.get('property_type'))

        title=response.xpath("//h3[@class='headline']/text()").get()
        if title:
            item_loader.add_value("title",title)
        item_loader.add_value("address","IXELLES")
        description=response.xpath("//div[@id='desc']//p//text()").get()
        if description:
            item_loader.add_value("description",description)
        external_id=response.xpath("//p[contains(.,'Réf.')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        rent=response.xpath("//li[contains(.,'Prix')]/text()").get()
        if rent:
            rent=rent.split(":")[-1].split("€")[0].replace(".","").strip()
            if not rent.isalpha():
                item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        room_count=response.xpath("//li[contains(.,'Chambres')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        bathroom_count=response.xpath("//li[contains(.,'Salle de bains')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip().split(" ")[0])
        square_meters=response.xpath("//li[contains(.,'Surface habitable')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[-1].split("m²")[0])
        terrace=response.xpath("//li[contains(.,'Terrasse')]/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        img=[]
        images=response.xpath("//img//@src").getall()
        if images:
            for i in images:
                if "phpThumb" in i:
                    img.append(i)
                    item_loader.add_value("images",img)
        energy_label=response.xpath("//img[contains(@src,'-energie')]/following-sibling::text()").get()
        if energy_label:
            energy = energy_label.replace("\r","").replace("\n","").split("k")[0]
            item_loader.add_value("energy_label",energy_label_calculate(int(float(energy.replace(",",".")))))
        item_loader.add_value("landlord_name","Avenues Immobilier")
        item_loader.add_value("landlord_phone","02 347 78 88")
        item_loader.add_value("landlord_email","info@avenues.be")
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