# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'agence_gramaglia_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Agence_Gramaglia_PySpider_france'

    def start_requests(self):
        yield Request("http://www.agence-cap-mala.fr/locations", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@id,'ID-')]//a[@class='hover-effect']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
        
        next_button = response.xpath("//a[@rel='Next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = " ".join(response.xpath("//div[contains(@class,'property-description')]/p//text()").getall()).strip()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        elif "piece" in response.url:
            item_loader.add_value("property_type", "apartment")
        else: return
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agence_Gramaglia_PySpider_france", input_type="VALUE")

        address = response.xpath("//h1/text()").get()
        if address and "vide" in address.lower():
            item_loader.add_value("address", "Vide")
            item_loader.add_value("city", "Vide")
        
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'property-description')]//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@class='media-body'][contains(.,'Surface')]/text()[2]", input_type="F_XPATH", get_num=True, split_list={"m":0, ".":0})
        if response.xpath("//div[@class='media-body'][contains(.,'Chambre')]/text()[1]"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='media-body'][contains(.,'Chambre')]/text()[1]", input_type="F_XPATH", get_num=True)
        elif "studio" in property_type:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="1", input_type="VALUE", get_num=True)
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value=response.url, input_type="VALUE", get_num=True, split_list={"-piece":0, "/":-1})
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='media-body'][contains(.,'Salle')]/text()[1]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='item-price']", input_type="F_XPATH", get_num=True, replace_list={",":"", "€":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'Dépot')]/text() | //strong[contains(.,'Caution')]/following-sibling::text()", input_type="F_XPATH", get_num=True, split_list={" ":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='media-body'][contains(.,'Référence')]/text()[2]", input_type="F_XPATH")
        item_loader.add_xpath("energy_label", "//div[@id='dpe_dpe']//b/text()")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//figure[@class='item-thumb']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'property_lat":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'property_lng":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Provisions s/charges')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Ascenseur')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrasse')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//li[contains(.,'Piscine')]", input_type="F_XPATH", tf_item=True)
        
        if "meuble" in response.url:
            item_loader.add_value("furnished", True)
            
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Agence Immobilière Cap Mala", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="33 04 93 78 20 20", input_type="VALUE")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "local" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("studio" in p_type_string.lower() or "t1" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "t2" in p_type_string.lower() or "t3" in p_type_string.lower() or "t4" in p_type_string.lower() or "t5" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None