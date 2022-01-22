# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from re import S
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
    name = 'imobook_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="imobook_PySpider_france"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.imobook.fr/ajax/ListeBien.php?page=1&tri=DTE_CREA&TypeModeListeForm=text&tdp=5&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=0&DataConfig=JsConfig.GGMap.Liste&Pagination=0",
            },           
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,)


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@itemprop='url']/@href").getall():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item,
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//h2[@class='detail-bien-type']/text()").get()
        if property_type and "maison" in property_type.lower():
            item_loader.add_value("property_type","house")
        rent=response.xpath("//span[.='Loyer Charges Comprises']/following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("par")[0].split("€")[0].strip().replace(" ",""))
        item_loader.add_value("currency","GBP")
        adres=response.xpath("//h2[@class='detail-bien-ville']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        zipcode=response.xpath("//h2[@class='detail-bien-ville']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split("(")[1].split(")")[0])
        square_meters=response.xpath("//span[@class='ico-surface']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        room_count=response.xpath("//span[@class='ico-piece']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        deposit=response.xpath("//span[.='Dépôt de garantie :']/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.replace(" ",""))
        description="".join(response.xpath("//span[@itemprop='description']//p//text()").get())
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//img[@class='photo-slideshow photo-thumbs']//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        energy_label=response.xpath("//img[@class='img-nrj img-dpe']/@src").get()
        if energy_label:
            energy_label = int(float(energy_label.split('WEB&consommation=')[-1].split("&emis")[0].strip().replace('%2C', '.')))
            if energy_label <= 50:
                item_loader.add_value("energy_label", 'A')
            elif energy_label >= 51 and energy_label <= 90:
                item_loader.add_value("energy_label", 'B')
            elif energy_label >= 91 and energy_label <= 150:
                item_loader.add_value("energy_label", 'C')
            elif energy_label >= 151 and energy_label <= 230:
                item_loader.add_value("energy_label", 'D')
            elif energy_label >= 231 and energy_label <= 330:
                item_loader.add_value("energy_label", 'E')
            elif energy_label >= 331 and energy_label <= 450:
                item_loader.add_value("energy_label", 'F')
            elif energy_label >= 451:
                item_loader.add_value("energy_label", 'G')
        item_loader.add_value("landlord_name","IMOBOOK")
        item_loader.add_value("landlord_phone"," 04 30 17 30 87")
        yield item_loader.load_item()