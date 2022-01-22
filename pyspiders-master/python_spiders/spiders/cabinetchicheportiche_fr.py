# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from itemadapter.utils import is_scrapy_item
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider): 
    name = 'cabinetchicheportiche_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Cabinetchicheportiche_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cabinetchicheportiche.fr/location/appartement?prod.prod_type=appt",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,callback=self.parse,meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='_gozzbg']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]}) 
        
    
    
    # 2. SCRAPING level 2 
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title","//title//text()")

        rent=response.xpath("//span[contains(.,'mois')]/preceding-sibling::text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace(" ","").strip())
        item_loader.add_value("currency","EUR")
        adres=response.xpath("//span[.='Localisation']/following-sibling::span/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=response.xpath("//span[.='Localisation']/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("city",city.split(" ")[0])
        zipcode=response.xpath("//span[.='Localisation']/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(" ")[1])
        external_id=response.xpath("//span[.='Référence']/following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        room_count=response.xpath("//span[contains(.,'Pièces')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[contains(.,'Salle d')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        square_meters=response.xpath("//span[contains(.,'Surface')]/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.strip())
        description="".join(response.xpath("//span[@class='_5qdxlk _5k1wy textblock ']//text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//div[@class='_17wyal9']//img/@data-src").getall()]
        if images:
            item_loader.add_value("images",images)
        floor=response.xpath("//span[.='Étage']/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        phone=response.xpath("//div[@class='_18g92fr  ']/a/@href").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.split(":")[1])

        rented = response.xpath("//div[text()='Loué']").get()
        if rented:
            return

        item_loader.add_value("landlord_name","cabinet chicheportiche")

        furnished_cond = response.xpath("//span[text()='Ameublement']/following-sibling::span/text()").get()
        if "Non" not in furnished_cond:
            item_loader.add_value("furnished",True)

        info_box = response.xpath("//span[@class='_ddbd1s _5k1wy textblock ']").get()
        if info_box:

            utilities = re.search("([\d.]+) €/mois",info_box)
            if utilities:
                utilities = utilities.group(1).split(".")[0]
                item_loader.add_value("utilities",utilities)

            deposit = re.search("garantie ([\d.]+)", info_box)
            if deposit:
                deposit = deposit.group(1).split(".")[0]
                item_loader.add_value("deposit",deposit)

        yield item_loader.load_item()