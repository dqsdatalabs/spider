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
    name = 'start_dijon_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        yield Request(
            "https://start-dijon.fr/locations",
            callback=self.parse,
        )

   
    # 1. FOLLOWING
    def parse(self, response):
        if response.xpath(".//div[contains(@class,'container ')]//div[@class='error']/text()").get():
            pass
        else:
            for url in response.xpath("//div[contains(@class,'container ')]//a[@class='property-item']/@href").getall():
                yield Request(response.urljoin(url), callback=self.populate_item)
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Start_Dijon_PySpider_france", input_type="VALUE")
        item_loader.add_value("external_link", response.url)
        # item_loader.add_value("property_type", response.meta["property_type"])
        property_type=response.xpath("//section/div[@class='title']/text()").get()
        if property_type and "appartement" in property_type.lower():
            item_loader.add_value("property_type","apartement")
        
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//section/div[@class='title']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//section//div[@class='price']/text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='value'][contains(.,'Localisation')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='value'][contains(.,'pièce')]//text()", input_type="M_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='value'][contains(.,'Salle de bain')]//text()", input_type="M_XPATH", get_num=True, split_list={":":1})
        
        square_meters = "".join(response.xpath("//div[@class='value'][contains(.,'Superficie')]/text()").getall())
        if square_meters:
            square_meters = square_meters.split(":")[1].strip().split(" ")[0].replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        external_id = response.xpath("//div[@class='reference']//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        desc = " ".join(response.xpath("//div[@class='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//img/@src[contains(.,'dpe')]", input_type="F_XPATH", split_list={"dpe/":1, "-":0})
        
        if "Adresse" in desc:
            address = desc.split("Adresse:")[1].split("DPE")[0].strip()
            item_loader.add_value("address", address)
        
        if "de garantie" in desc:
            deposit = desc.split("de garantie:")[1].split("\u20ac")[0].strip()
            item_loader.add_value("deposit", deposit)
        
        if "de charges" in desc:
            utilities = desc.split("de charges")[0].strip().split(" ")[-1].replace("\u20ac","")
            item_loader.add_value("utilities", utilities)
            
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'gallery-top')]//@src", input_type="M_XPATH")
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@class='informations']/div[@class='name']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='informations']/div[@class='phone']/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//div[@class='informations']/div[@class='email']/text()", input_type="F_XPATH", split_list={":":1})

        
        
        yield item_loader.load_item()
