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
    name = 'bochuimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    start_urls = ['https://www.bochuimmo.com/tout-location.html']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//ul[@class='liste-items']//li"):
            f_url = item.xpath("./article[contains(@class,'liste-item')]/@onclick").get()
            prop_type = "".join(item.xpath("./article//p[@class='liste-item-desc']//text()").getall())
            if get_p_type_string(prop_type):
                property_type = get_p_type_string(prop_type)
                yield Request(response.urljoin(f_url.split('"')[1]), callback=self.populate_item, meta={"property_type":property_type})
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        title = " ".join(response.xpath("//h3[contains(@class,'detail-offre')]//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Bochuimmo_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h2[contains(@class,'header')]/text()", input_type="M_XPATH", split_list={"-":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h2[contains(@class,'header')]/text()[contains(.,'(')]", input_type="F_XPATH", split_list={"(":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h2[contains(@class,'header')]/text()[contains(.,'(')]", input_type="F_XPATH", split_list={"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[contains(@class,'-texte')]/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//h3[contains(@class,'detail-offre')]//text()[contains(.,'m²')]", input_type="F_XPATH", get_num=True, split_list={"m²":0, " ":-1})
        
        if response.xpath("//ul[contains(@class,'detail')]/li[contains(.,'chambre')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//ul[contains(@class,'detail')]/li[contains(.,'chambre')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//h3[contains(@class,'detail-offre')]//text()[contains(.,'pièce')]", input_type="F_XPATH", get_num=True, split_list={"pièce":0, " ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//ul[contains(@class,'detail')]/li[contains(.,'salle')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[@class='detail-offre-prix']/text()", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p//text()[contains(.,'Dépôt')]", input_type="F_XPATH", get_num=True, split_list={":":1, "€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//h2[contains(@class,'header')]/text()[contains(.,'réf')]", input_type="F_XPATH", split_list={"réf.":1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='gallery2']//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li//text()[contains(.,'de provisions')]", input_type="F_XPATH", get_num=True, split_list={"dont":1, "€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//ul[contains(@class,'detail')]/li[contains(.,'parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Ascenseur')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//ul[contains(@class,'detail')]/li[contains(.,'terrasse')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="GERARD BOCHU IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 67 79 44 10", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="bochuimmo.loc@orange.fr", input_type="VALUE")
        
        energy_label = response.xpath("//h5/a/text()[contains(.,'Classe')]").get()
        if energy_label:
            energy_label = energy_label.split("-")[0].split(")")[1].strip()
            item_loader.add_value("energy_label", energy_label)
                
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "unit" in p_type_string.lower() or "home" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    else:
        return None