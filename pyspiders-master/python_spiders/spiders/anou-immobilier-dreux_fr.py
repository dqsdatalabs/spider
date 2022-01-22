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
    name = 'anou-immobilier-dreux_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="AnouImmobilierDreux_PySpider_france"
    custom_settings = {"HTTPCACHE_ENABLED": False}

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.anou-immobilier.fr/catalog/advanced_search_result.php?action=update_search&search_id=1719372845841594&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27=1&C_27_search=EGAL&C_27_type=UNIQUE&C_65_search=CONTIENT&C_65_type=TEXT&C_65=0&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MIN_REPLACE=&C_30_MAX=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&keywords=&C_124_search=EGAL&C_124_type=UNIQUE&C_124=",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "http://www.anou-immobilier.fr/catalog/advanced_search_result.php?action=update_search&search_id=1719372845841594&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27=2&C_27_search=EGAL&C_27_type=UNIQUE&C_65_search=CONTIENT&C_65_type=TEXT&C_65=0&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MIN_REPLACE=&C_30_MAX=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&keywords=&C_124_search=EGAL&C_124_type=UNIQUE&C_124=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,meta={"property_type":url.get('property_type')})
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='photo']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={'property_type': response.meta.get('property_type')})
        nextpage=response.xpath("//li/a[@class='page_suivante']/@href").get()
        if nextpage:
            yield Request(url=response.urljoin(nextpage),callback=self.parse,meta={'property_type': response.meta.get('property_type')})
            

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        description=response.xpath("//p[@itemprop='description']/text()").get()
        if description:
            item_loader.add_value("description",description)
        external_id=response.xpath("//div[.='Référence']/following-sibling::div/b/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        square_meters=response.xpath("//div[.='Surface habitable']/following-sibling::div/b/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].split(".")[0].strip())
        room_count=response.xpath("//div[.='Nbre de pièces']/following-sibling::div/b/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        zipcode=response.xpath("//div[.='Code postal']/following-sibling::div/b/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip())
        city=response.xpath("//div[.='Ville']/following-sibling::div/b/text()").get()
        if city:
            item_loader.add_value("city",city.replace("\n","").strip())
        adres=city+" "+zipcode
        if adres:
            item_loader.add_value("address",adres.strip())
        energy_label=response.xpath("//img[contains(@src,'DPE')]/@src").get()
        if energy_label:
            energy_label=energy_label.split("DPE_")[-1].split("_")[0]
            if not "vierge" in energy_label:
                item_loader.add_value("energy_label",energy_label)
        images=[response.urljoin(x) for x in response.xpath("//div[@class='box_img']/a/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        rent=response.xpath("//span[@class='alur_loyer_price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("Loyer")[-1].split("€")[0])
        item_loader.add_value("currency","EUR")
        utilities=response.xpath("//div[.='Honoraires Locataire']/following-sibling::div/b/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("EUR")[0].strip())
        deposit=response.xpath("//div[.='Dépôt de Garantie']/following-sibling::div/b/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("EUR")[0].strip())
        bathroom_count=response.xpath("//div[contains(.,'Salle(s) d')]/following-sibling::div/b/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        terrace=response.xpath("//div[.='Jardin']/following-sibling::div/b/text()").get()
        if terrace and "non" in terrace.lower():
            item_loader.add_value("terrace",False)
        if terrace and "oui" in terrace.lower():
            item_loader.add_value("terrace",True)
        item_loader.add_value("landlord_name","Anou Immobilier")
        phone=response.xpath("//i[@class='fa fa-phone-square']/following-sibling::text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.split(":")[-1].strip())

        yield item_loader.load_item()