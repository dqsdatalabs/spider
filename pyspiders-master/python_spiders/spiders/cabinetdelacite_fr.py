# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy

class MySpider(Spider):
    name = 'cabinetdelacite_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Cabinetdelacite_PySpider_france'

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.cabinetdelacite.fr/resultats?transac=location&type%5B%5D=appartement&budget_maxi=&surface_mini=&ref_bien=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.cabinetdelacite.fr/resultats?transac=location&type%5B%5D=maison&budget_maxi=&surface_mini=&ref_bien=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//script[contains(.,'\"lien\"')]/text()").extract():
            follow_url = item.split('"lien": "')[1].split('",')[0]

            if 'location' in follow_url: yield Request(response.urljoin(follow_url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value=self.external_source, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[contains(@class,'small')]//text()", input_type="F_XPATH", split_list={".":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//i[contains(@class,'location')]//parent::address//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//i[contains(@class,'location')]//parent::address//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1[contains(@class,'titre')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//h1[contains(@class,'titre')]//following-sibling::p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//strong[contains(.,'Surface')]//parent::li/text()", input_type="F_XPATH", get_num=True)
        if response.xpath("//li[contains(.,'chambre')]//strong//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'chambre')]//strong//text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//li[contains(.,'pièce')]//strong//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'pièce')]//strong//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'Salle')]//strong//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h3//span[contains(.,'€')]/text()", input_type="F_XPATH", get_num=True, replace_list={"€":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'Dépôt de garantie')]//strong//text()", input_type="F_XPATH", get_num=True, replace_list={"€":""})

        utilities = "".join(response.xpath("//h1[contains(@class,'titre')]//following-sibling::p//text()").getall())
        if utilities and "dont" in utilities:
            utilities = utilities.split("dont")[1].split("€")[0].split(",")[0].strip()
            if utilities.isdigit():
                item_loader.add_value("utilities", utilities)
            
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'slides')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[contains(.,'Etage')]//strong//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Garage')]//strong//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon')]//strong//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Meublé')]//text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Ascenseur')]//text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="CABINET DE LA CITE", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 68 51 54 73", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="location@cabinetdelacite.fr", input_type="VALUE")
   
        yield item_loader.load_item()