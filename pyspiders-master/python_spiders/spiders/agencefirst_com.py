# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'agencefirst_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    custom_settings = {
        "PROXY_ON": True,
    }
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agencefirst.com/immobilier/location-type/appartement-categorie/1p-pieces/",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='liste-items']/li/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agencefirst_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//h3/small//text()[contains(.,'m²')]", input_type="F_XPATH", get_num=True, split_list={"m²":0, " ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[@class='detail-offre-prix']/text()", input_type="M_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        
        if response.xpath("//li[contains(.,'chambre')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'chambre')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        elif "".join(response.xpath("//h3/small//text()[contains(.,'pièce')]").getall()):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//h3/small//text()[contains(.,'pièce')]", input_type="F_XPATH", get_num=True, split_list={"pièce":0, " ":-1})
        elif "".join(response.xpath("//h3/small//text()[contains(.,'studio')]").getall()):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="1", input_type="VALUE")
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'salle')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//title/text()", input_type="M_XPATH", split_list={",":0}, replace_list={"Appartement":""})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//title/text()", input_type="M_XPATH", split_list={",":0, " ":1}, replace_list={"Appartement":""})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h2/text()[contains(.,'(')]", input_type="M_XPATH", split_list={"(":1, ")":0})
        
        desc = " ".join(response.xpath("//p[@class='detail-offre-texte']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'terrasse')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Ascenseur')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//ul[contains(@class,'liste-frais-charges')]//text()[contains(.,'garantie')]", input_type="M_XPATH", get_num=True, split_list={"garantie":0, "€":-2, " ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//ul[contains(@class,'liste-frais-charges')]//text()[contains(.,'de provisions')]", input_type="M_XPATH", get_num=True, split_list={"dont":1, "€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng: Number(')]/text()", input_type="F_XPATH", split_list={"lat: Number(":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng: Number(')]/text()", input_type="F_XPATH", split_list={"lng: Number(":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//h2//text()[contains(.,'réf')]", input_type="F_XPATH", split_list={"réf.":1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='gallery2']//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@class='widget-testimonial-content']/h4/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='widget-testimonial-content']//a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@agencefirst.com", input_type="VALUE")
        
        yield item_loader.load_item()