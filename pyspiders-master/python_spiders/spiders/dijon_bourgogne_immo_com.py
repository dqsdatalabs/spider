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
    name = 'dijon_bourgogne_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Dijon_Bourgogne_Immobilier_PySpider_france'
    custom_settings={
        "PROXY_PR_ON": True,
    }

    start_urls = ["https://dijon-bourgogne-immobilier.com/biens+a+louer+dijon.php"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='un-bien']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url.split("?")[0])
        f_text = response.url
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[@class='col-sm-8']//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: 
            return
        
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Dijon_Bourgogne_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//li[contains(.,'Référence')]/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h2[@id='prix']/text()", input_type="F_XPATH", split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charge')]/span/text()", input_type="F_XPATH", split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p/strong[contains(.,'de garantie')]/following-sibling::text()", input_type="F_XPATH", split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//img[contains(@src,'DPE-')]/@src", get_num=True, input_type="F_XPATH", split_list={"DPE-":1, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@id='vignettes']//@src", input_type="M_XPATH")
        
        room_count = response.xpath("//li[contains(.,'chambre')]/span/text()").get()
        if room_count:
            item_loader.add_value('room_count', room_count)
        else:
            room_count = response.xpath("//li[contains(.,'Nombre de pièces')]/span/text()").get()
            if room_count:
                item_loader.add_value('room_count', room_count)
        
        square_meters = response.xpath("//li[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        desc = " ".join(response.xpath("//p[@id='descriptif']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "\u00e9tage" in desc:
            floor = desc.split("\u00e9tage")[0].strip().split(" ")[-1].replace("ème","").replace("er","")
            if floor.isdigit():
                item_loader.add_value("floor", floor)
        
        address = response.xpath("//h1/text()").get()
        if address:
            address = address.strip().split(" ")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="DIJON BOURGOGNE IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="03 80 45 38 25", input_type="VALUE")

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "apartment" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "détachée" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    elif p_type_string and "suite" in p_type_string.lower():
        return "room"
    else:
        return None