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
    name = 'lohr_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Lohr_Immo_PySpider_france"
    def start_requests(self):
        start_urls = ["https://www.lohr-immo.com/?a=1&t=0&y=3201&r=0&n=168&i=0&c=10&v=list&o=&s="]
        yield Request(start_urls[0], callback=self.parse)   

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'immo-list-element')]//a[@class='speciallink']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)

        next_button = response.xpath("//a[@class='pagination' and contains(.,'Next')]/@onclick").get()
        if next_button: yield Request(response.urljoin(next_button.split("core.http.redirect('")[-1].split("'")[0]), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = "".join(response.xpath("//div[contains(text(),'Type de bien')]/following-sibling::div[1]//text()").getall())
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//div[@class=' label'][contains(.,'Type')]/following-sibling::div/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        else:
            title = response.xpath("//title//text()").get()
            if title:
                title = re.sub('\s{2,}', ' ', title.strip())
                item_loader.add_value("title", title)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'detailsleft')][contains(.,'Loyer')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True, split_list={"EUR":0}, replace_list={".":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p/span[contains(.,'garantie')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1, "€":0}, replace_list={".":""})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//p/span[contains(.,'Charges:')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1, "€":0}, replace_list={".":""})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[contains(@class,'l138')]//text()[contains(.,'Numéro')]", input_type="F_XPATH", split_list={"Numéro ":1, "-":0})
        
        room_count = response.xpath("//title//text()[contains(.,'chambre')] | //div/p//text()[contains(.,'chambre')] | //h1//text()[contains(.,'chambre') or contains(.,'Chambre')]").get()
        if room_count:
            room_count = room_count.lower().split("chambre")[0].replace("gr.","").replace("grandes","").strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            elif response.xpath("//p[contains(.,'chambre')]/text()").get():
                item_loader.add_value("room_count", "1")

        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//title//text()[contains(.,'salle d')]", input_type="F_XPATH", split_list={"salle d":0, " ":-1})
        
        address = "".join(response.xpath("//div[contains(@class,'label')][contains(.,'Lie')]/following-sibling::div//text()").getall())
        if address:
            if "/" in address:
                address = address.split("/")[0].strip()
                if "," in address:
                    item_loader.add_value("address", address.split(",")[0].replace("A env. 5 - 15 min","").strip())
                    item_loader.add_value("city", address.split(",")[0].replace("A env. 5 - 15 min","").strip())
                else:
                    item_loader.add_value("address", address)
                    item_loader.add_value("city", address)
            else:
                address = address.split(",")[0]
                if "." in address:
                    item_loader.add_value("address", address.split(".")[0].strip())
                    item_loader.add_value("city", address.split(".")[0].strip())
                else:
                    item_loader.add_value("address", address)
                    item_loader.add_value("city", address)
                    
        square_meters = response.xpath("//h1//text()[contains(.,'m²')]").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters)
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[contains(@class,'detailsleft')][contains(.,'Surface')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True, split_list={"m²":0, "sqm":0 , " ":-1})
            
        floor = "".join(response.xpath("//div[@class='border-t print']/div//strong[contains(.,'Détails')]/parent::p/parent::div//text()[contains(.,'étage')]").getall())
        if floor:
            floor = floor.split("étage")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)
            
        energy_label = response.xpath("//p/span[contains(.,'DPE')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[1].strip())
        
        desc = " ".join(response.xpath("//meta[@name='description']/@content").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@class='border-t print']/div//strong[contains(.,'Détails')]/parent::p/parent::div//text()[contains(.,'terrasse')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@class='border-t print']/div//strong[contains(.,'Détails')]/parent::p/parent::div//text()[contains(.,'parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[@class='border-t print']/div//strong[contains(.,'Détails')]/parent::p/parent::div//text()[contains(.,'ascenseur')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'pictures')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="LOHR - Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="33 3 89 70 87 14", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="welo2@wanadoo.fr", input_type="VALUE")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "appartement" in p_type_string.lower():
        return "apartment"
    elif p_type_string and "maison" in p_type_string.lower():
        return "house"
    else:
        return None