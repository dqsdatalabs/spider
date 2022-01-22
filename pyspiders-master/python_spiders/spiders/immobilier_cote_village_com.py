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
    name = 'immobilier_cote_village_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "1",
            },
            {
                "property_type" : "house",
                "type" : "2",
            },
        ]
        for item in start_urls:
            formdata = {
                "nature": "2",
                "type[]": item["type"],
                "price": "",
                "age": "",
                "tenant_min": "",
                "tenant_max": "",
                "rent_type": "",
                "newprogram_delivery_at": "",
                "newprogram_delivery_at_display": "",
                "currency": "EUR",
                "customroute": "",
                "homepage": "",
            }
            api_url = "https://www.immobilier-cote-village.com/fr/recherche/"
            yield FormRequest(
                url=api_url,
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                })

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='buttons']//a[@class='button']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Immobilier_Cote_Village_PySpider_france", input_type="VALUE")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//li[contains(.,'Ref')]/text()", input_type="F_XPATH", split_list={".":1})
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(.,'Surface')]/span/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        if response.xpath("//li[contains(.,'Chambre')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Chambre')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Pièce')]/span/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//article/div/h2//text()[2]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//article/div/h2//text()[2]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li[contains(.,'Mois')]/text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'salle')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'de garantie')]/span/text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges')]/span/text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'L.marker')]/text()", input_type="F_XPATH", split_list={'L.marker([':2, ',':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'L.marker')]/text()", input_type="F_XPATH", split_list={'L.marker([':2, ',':1, ']':0})
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrasse') or contains(.,'terrasse')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Ascenseur')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Garage')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Meublé')]//text()", input_type="F_XPATH", tf_item=True)
        
        energy_label = response.xpath("//img/@src[contains(.,'diag')]").get()
        if energy_label:
            energy_label = energy_label.split("/")[-1]
            if energy_label.isdigit() and energy_label != "-1":
                item_loader.add_value("energy_label", energy_label)
        
        desc = " ".join(response.xpath("//p[@id='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'Disponible')]/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[contains(.,'Etage')]/span/text()[not(contains(.,'Rez'))]", input_type="F_XPATH", split_list={" ":0}, replace_list={"ème":"", "er":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//section[@class='showPictures']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@class='userBlock']//strong/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='userBlock']//a[contains(@href,'tel')]/text()", input_type="F_XPATH", replace_list={"+":""})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//div[@class='userBlock']//a[contains(@href,'mail')]/text()", input_type="F_XPATH")

        yield item_loader.load_item()
