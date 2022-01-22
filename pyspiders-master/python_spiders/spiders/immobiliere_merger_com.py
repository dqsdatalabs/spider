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
    name = 'immobiliere_merger_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_url = "http://www.immobiliere-merger.com/fr/recherche/"
        payloads = [
            {
                "payload" : "nature=2&type%5B%5D=1&currency=EUR",
                "property_type" : "apartment",
            },
            {
                "payload" : "nature=2&type%5B%5D=2&currency=EUR",
                "property_type" : "house"
            },
        ]
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Origin': 'http://www.immobiliere-merger.com',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Referer': 'http://www.immobiliere-merger.com/fr/recherche/',
            'Accept-Language': 'tr,en;q=0.9',
        }
        for item in payloads:
            yield Request(start_url, method="POST", callback=self.parse, headers=headers, body=item["payload"], meta={'property_type': item['property_type']})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='ads']/li//a[contains(.,'Vue détaillée')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    
        next_button = response.xpath("//li[@class='nextpage']/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = " ".join(response.xpath("//article/div/h2/text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Immobiliere_Merger_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//article/div/h2/text()[2]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//article/div/h2/text()[2]", input_type="F_XPATH")
        
        if response.xpath("//ul/li[contains(.,'chambre')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//ul/li[contains(.,'chambre')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//ul/li[contains(.,'Pièce')]/span/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//article/div/ul/li[contains(.,'€')]/text()", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//ul/li[contains(.,'salle')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//ul/li[contains(.,'Surface')]/span/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//ul/li[contains(.,'Disponible')]/span/text()", input_type="F_XPATH", get_num=True)

        floor = response.xpath("//ul/li[contains(.,'Etage')]/span/text()").get()
        if floor:
            floor = floor.split("/")[0].strip().replace("ème","")
            if floor.isdigit():
                item_loader.add_value("floor", floor)
                
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//ul/li[contains(.,'Balcon')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//ul/li[contains(.,'Charges')]/span/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//ul/li[contains(.,'garantie')]/span/text()", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'L.marker')]/text()", input_type="F_XPATH", split_list={"L.marker([":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'L.marker')]/text()", input_type="F_XPATH", split_list={"L.marker([":1, ",":1, "]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//ul/li[contains(.,'Ref')]/text()", input_type="F_XPATH", split_list={".":1})
        
        desc = " ".join(response.xpath("//p[@class='comment']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'show-carousel')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//ul/li[contains(.,'Ascenseur')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//ul/li[contains(.,'Terrasse')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//ul/li[contains(.,'Garage') or contains(.,'Parking')]/text()", input_type="F_XPATH", tf_item=True)
        
        energy_label = response.xpath("//img[contains(@src,'diag')]/@src").get()
        if energy_label:
            if "%2" in energy_label:
                item_loader.add_value("energy_label", energy_label.split("/")[-1].split("%")[0])
            else:
                item_loader.add_value("energy_label", energy_label.split("/")[-1])
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="IMMOBILIERE MERGER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//p/span[contains(@class,'phone')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@immomerger.com", input_type="VALUE")
             
        yield item_loader.load_item()