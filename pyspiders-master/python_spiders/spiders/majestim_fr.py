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
from python_spiders.helper import ItemClear

class MySpider(Spider):
    name = 'majestim_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Majestim_PySpider_france"
    def start_requests(self):
        start_url = "https://www.majestim.fr/nos-biens/"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//article[contains(@class,'property-category-locations')]//a[@target='_self']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.xpath("//div[contains(@style,'margin-top')]/h1[1]/text()").get()
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        item_loader.add_value("external_link", response.url)
        externalid=response.xpath("//link[@rel='shortlink']/@href").get() 
        if externalid:
            externalid=externalid.split("p=")[-1]
            item_loader.add_value("external_id",externalid)
        item_loader.add_value("external_source",self.external_source)
        # ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Majestim_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[contains(@class,'wpb_content_element')]//h1//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div/img[contains(@src,'icon-quartier')]/parent::div/parent::div/following-sibling::div//text()", input_type="M_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div/img[contains(@src,'icon-quartier')]/parent::div/parent::div/following-sibling::div//text()", input_type="M_XPATH")
        city="".join(response.xpath("//div/img[contains(@src,'icon-quartier')]/parent::div/parent::div/following-sibling::div//text()").getall())
        if city:
            city=city.split("-")[0].replace("\n","").strip()
            city=city.split(" ")[0]
            item_loader.add_value("city",city)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div/img[contains(@src,'icon-euro')]/parent::div/parent::div/following-sibling::div//text()", input_type="M_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div/img[contains(@src,'icon-surface')]/parent::div/parent::div/following-sibling::div//text()", input_type="M_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div/img[contains(@src,'icon-chambre')]/parent::div/parent::div/following-sibling::div//text()", input_type="M_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div/img[contains(@src,'icon-dpe')]/parent::div/parent::div/following-sibling::div//text()", input_type="M_XPATH", get_num=True, split_list={" ":0})
        
        desc = " ".join(response.xpath("//div[contains(@class,'wpb_content_element')]//p[not(contains(.,'Remplissez'))]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "salle de bain" in desc:
            bathroom_count = desc.split("salle de bain")[0].strip().split(" ")[-1]
            if "une" in bathroom_count:
                item_loader.add_value("bathroom_count", "1")
        
        if " \u00e9tage" in desc:
            floor = desc.split(" \u00e9tage")[0].strip().split(" ")[-1].replace("e","")
            item_loader.add_value("floor", floor)
            
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//a[@itemprop='image']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="MAJESTIM PARIS REALTY", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01 47 20 88 61", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@majestim.fr", input_type="VALUE")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "appartement" in p_type_string.lower():
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    else:
        return None