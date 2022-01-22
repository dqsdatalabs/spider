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
import dateparser
from datetime import datetime

class MySpider(Spider):
    name = 'buchy_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Buchy_Immobilier_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.buchy-immobilier.com/louer/appartements/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.buchy-immobilier.com/louer/maisons/",
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

        for item in response.xpath("//ul[contains(@class,'cat--list')]/li//a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[@title='Suivant']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Buchy_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//article/h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p[contains(@class,'ville')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p[contains(@class,'ville')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Nb de pièce(s) :')]/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'salle')]/span/text()[.!='0']", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(.,'Surface totale')]/span/text()", input_type="F_XPATH", split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'tarif')]/text()", input_type="M_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[contains(.,'étage')]/span/text()[.!='0']", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Ascenseur')]/span/text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrasse')]/span/text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[contains(@class,'ref')]/span/text()", input_type="F_XPATH")
        
        energy_label = response.xpath("//span[@class='diagnos--number']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        desc = " ".join(response.xpath("//div[@class='prod--desc']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        available_date=response.xpath("//div[contains(text(),'Libre au')]/text()").get()
        if available_date:
            available_date=available_date.split("Libre au")[-1].strip() 
            if available_date: 
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %B %Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

            
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'parking')]/span/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p[contains(@class,'price')]//text()[contains(.,'garantie')]", input_type="F_XPATH", get_num=True, split_list={":":1, "€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//p[contains(@class,'price')]//text()[contains(.,'honoraires')]", input_type="F_XPATH", get_num=True, split_list={":":1, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='prod--slider']//@src", input_type="M_XPATH")
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="BUCHY IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="02 51 21 03 03", input_type="VALUE")

        yield item_loader.load_item()