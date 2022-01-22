# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from typing import NewType
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
    name = 'groupe-mercure_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="GroupeMercure_PySpider_france"
    custom_settings = {"HTTPCACHE_ENABLED": False}

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://groupe-mercure.fr/louer.html",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='link']//a[@class='btnstd fill']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,)

            

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title=response.xpath("//span[@class='main midline']/text()").get()
        if title:
            item_loader.add_value("title",title)
        
        property_type=response.xpath("//span[@class='main midline']/text()").get()
        if property_type:
            if "Maison" in property_type:
                item_loader.add_value("property_type","house")
            if "Appartement" in property_type:
                item_loader.add_value("property_type","apartment")
        address=response.xpath("//span[@class='main midline']/text()").get()
        if address:
            item_loader.add_value("address",address.split("à louer")[-1])
        rent=response.xpath("//span[@class='convert_currency']/@data-euro").get()
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        square_meters=response.xpath("//div[@class='feature surface']//div[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].split(",")[0].strip())
        room_count=response.xpath("//div[@class='feature nb_piece']//div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count=response.xpath("//div[@class='feature nb_bain']//div[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        energy_label=response.xpath("//div[@class='val etiquete_dpe_txt']/@data-dpe").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label_calculate(int(float(energy_label.replace(",",".")))))
        external_id=response.xpath("//div[@class='feature reference_bien']//div[2]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        description="".join(response.xpath("//div[@class='htmlstd mtrim']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        deposit=response.xpath("//div[@class='Depôt de garantie :']/following-sibling::div/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0].strip())
        utilities=response.xpath("//div[@class='Honoraires location :']/following-sibling::div/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].strip())
        images=[x for x in response.xpath("//div[@class='photo_item']/div/img/@data-src").getall()]
        if images:
            item_loader.add_value("images",images)
        latitude=response.xpath("//div[@class='googlemapbox']/following-sibling::script[contains(.,'location')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("jsonstyle")[0].split("location")[-1].split("lat")[-1].split(",")[0].replace(":",""))
        longitude=response.xpath("//div[@class='googlemapbox']/following-sibling::script[contains(.,'location')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("jsonstyle")[0].split("location")[-1].split("lng")[-1].split("}")[0].replace(":",""))
        phone=response.xpath("//a[@class='btnstd fill gtm_tel_btn']/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        item_loader.add_value("landlord_name","Groupe Mercure")
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