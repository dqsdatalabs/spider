# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'agencemci_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Agencemci_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agencemci.com/location-appartement-1.html",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agencemci.com/location-maison-1.html",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//li[@class='liste-item-wrapper']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        external_id = "".join(response.xpath("//h2[contains(@class,'detail-header-titre')]/text()").getall())
        if external_id:
            external_id = external_id.split("réf.")[1].strip()
            item_loader.add_value("external_id", external_id)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h2[contains(@class,'detail-header-titre')]/text()", input_type="M_XPATH", split_list={"-":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h2[contains(@class,'detail-header-titre')]/text()", input_type="M_XPATH", split_list={"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h2[contains(@class,'detail-header-titre')]/text()", input_type="M_XPATH",split_list={"(":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[contains(@class,'detail-offre-texte')]/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//h3[contains(@class,'detail-offre-titre')]//small//text()", input_type="M_XPATH", get_num=True, split_list={"m²":0," ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[contains(@class,'detail-offre-prix')]//text()", input_type="M_XPATH", get_num=True, split_list={"€":0,".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        if response.xpath("//li[contains(@class,'detail-offre-caracteristique')][contains(.,'chambres')]//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(@class,'detail-offre-caracteristique')][contains(.,'chambres')]//text()", input_type="M_XPATH", get_num=True, split_list={" ":0})
        elif "".join(response.xpath("//h3[contains(@class,'detail-offre-titre')]//small//text()").getall()):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//h3[contains(@class,'detail-offre-titre')]//small//text()", input_type="M_XPATH", get_num=True, split_list={"pièces":0,"pièce":0," ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(@class,'detail-offre-caracteristique')][contains(.,'salle')]//text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@id,'gallery')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(@class,'detail-offre-caracteristique')][contains(.,'parking')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(@class,'detail-offre-prestation')][contains(.,'Balcon')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(@class,'detail-offre-prestation')][contains(.,'Ascenseur')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(@class,'detail-offre-caracteristique')][contains(.,'terrasse')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Mathieu Contact Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="33 0 466 761 738", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="location@agencemci.com", input_type="VALUE")
        
        title = " ".join(response.xpath("//h3[contains(@class,'detail-offre-titre')]//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        charges = "".join(response.xpath("//ul[contains(@class,'detail-offre-liste-frais-charges')]//text()").getall())
        if charges:
            utilities = charges.split("de provisions")[0].strip().split(" ")[-1].split("\u00a0")[0]
            item_loader.add_value("utilities", utilities)
            deposit = charges.split("€")[-2].strip().split(" ")[-1].replace("\u00a0","")
            item_loader.add_value("deposit", deposit)

        energy_label = "".join(response.xpath("//a[contains(@aria-controls,'dpe-ges')]//text()[normalize-space()]").getall())
        if energy_label:
            energy_label = energy_label.split("(dpe)")[1].split("-")[0].strip()
            item_loader.add_value("energy_label", energy_label)

        yield item_loader.load_item()