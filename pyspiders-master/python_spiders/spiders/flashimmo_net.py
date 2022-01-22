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
    name = 'flashimmo_net'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://flashimmo.net/annonces-immobilieres-le-cannet.html?t=l&smin=&pmin=&v=all&b=a&smax=&pmax=&r=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://flashimmo.net/annonces-immobilieres-le-cannet.html?t=l&smin=&pmin=&v=all&b=m&smax=&pmax=&r=",
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
        for item in response.xpath("//a[@class='btn_annonce']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Flashimmo_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/span[last()]/text()", input_type="F_XPATH", split_list={"-":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1/span[last()]/text()", input_type="F_XPATH", split_list={"-":0, "(":-1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/span[last()]/text()", input_type="F_XPATH", split_list={"-":0, "(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//section[@id='description_bien']/div/div[1]//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//b[contains(.,'Surface habitable')]/following-sibling::span[1]/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//b[contains(.,'Nb. de chambre') or contains(.,'Nb. de pièces')]/following-sibling::span[1]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h1/span[1]/following-sibling::text()[1]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[@class='compc']/text()[contains(.,'Dépôt')]", input_type="F_XPATH", get_num=True, split_list={":":-1, ".":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='ei-slider']/ul[1]//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[@class='compc']/text()[contains(.,'Charges')]", input_type="F_XPATH", get_num=True, split_list={":":-1, ".":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//b[contains(.,'Ascenseur')]/following-sibling::span[1]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//b[contains(.,'Terrasse')]/following-sibling::span[1]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="FLASHIMMO", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="06.09.50.79.35", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@flashimmo.net", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//b[contains(.,'Référence')]/following-sibling::span[1]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//b[.='Étage : ']/following-sibling::span[1]/text()", input_type="F_XPATH", get_num=True)

        if response.xpath("//section[@id='description_bien']/div/div[1]//p//text()[contains(.,'salle de bain')]").get(): item_loader.add_value("bathroom_count", 1)
        
        energy_label = response.xpath("//img[@class='dpe']/@src").get()
        if energy_label:
            energy_label = int(energy_label.split('-')[1].strip()) if energy_label.split('-')[1].isnumeric() else None
            if energy_label:
                if energy_label <= 50: item_loader.add_value("energy_label", 'A')
                elif energy_label >= 51 and energy_label <= 90: item_loader.add_value("energy_label", 'B')
                elif energy_label >= 91 and energy_label <= 150: item_loader.add_value("energy_label", 'C')
                elif energy_label >= 151 and energy_label <= 230: item_loader.add_value("energy_label", 'D')
                elif energy_label >= 231 and energy_label <= 330: item_loader.add_value("energy_label", 'E')
                elif energy_label >= 331 and energy_label <= 450: item_loader.add_value("energy_label", 'F')
                elif energy_label >= 451: item_loader.add_value("energy_label", 'G')

        yield item_loader.load_item()