# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'agence_ottonello_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agence-ottonello.com/fr/locations-biens-immobiliers.htm?_typebase=2&_typebien%5B%5D=1&prixloyerchargecomprise%5B%5D=&prixloyerchargecomprise%5B%5D=&_motsclefs=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agence-ottonello.com/fr/locations-biens-immobiliers.htm?_typebase=2&_typebien%5B%5D=2&prixloyerchargecomprise%5B%5D=&prixloyerchargecomprise%5B%5D=&_motsclefs=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 1)
        for item in response.xpath("//a[@class='bouton']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[.='>']/@href").get()
        if next_page:
            p_url = response.url.split("&page=")[0] + f"&page={page}"
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        parking_type =  response.xpath("//div[@id='texte-detail']/strong/text()[contains(.,'PARKING EN EXTERIEUR') or contains(.,'Une place de parking -')]").get()
        if parking_type:
            return
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Wwww_Agence_Ottonello_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@id='reference-detail']//text()", input_type="F_XPATH",split_list={"Réf.":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[@id='image-dpe']/@style", input_type="F_XPATH",split_list={"/dpe-":-1,".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@id='lieu-detail']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[@id='lieu-detail']//text()", input_type="F_XPATH", split_list={" - ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@id='lieu-detail']//text()", input_type="F_XPATH", split_list={" - ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div[div[.='Etage']]/div[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@id='texte-detail']/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[div[.='Surface habitable']]/div[2]/text()", input_type="F_XPATH", get_num=True, split_list={".":0,"m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='champsSPEC-row'][div[contains(.,'Nombre de salles d')]]/div[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='champsSPEC-row'][div[contains(.,'Prix loyer hors charges')]]/div[2]/text()", input_type="F_XPATH", get_num=True, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[@class='financierStd']/span[contains(.,'Dépôt de garantie')]/following-sibling::text()[1]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[@class='financierStd']/span[contains(.,'Provisions charges')]/following-sibling::text()[1]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'diaporama-photo-immobilier')]/div//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@id='texte-detail']/strong/text()[contains(.,'Parking') or contains(.,'garage')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@id='texte-detail']/strong/text()[contains(.,'terrasse')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@id='texte-detail']/strong/text()[contains(.,'Meublé')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[@id='texte-detail']/strong/text()[contains(.,'balcon ')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Agence Ottonello", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="+33 (0)4 94 45 70 44", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="location@ottonello.fr", input_type="VALUE")    
        room_count = response.xpath("//div[div[.='Nombre de chambres']]/div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//div[div[.='Nombre de pièces']]/div[2]/text()")
        available_date = response.xpath("//div[@id='texte-detail']//text()[contains(.,'Libre au')]").get()
        if available_date:
            available_date= available_date.split("Libre")[-1].replace("au","").replace("le","").strip().split(" ")[0]
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        yield item_loader.load_item()