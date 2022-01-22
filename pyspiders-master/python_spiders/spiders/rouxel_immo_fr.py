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
    name = 'rouxel_immo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        formdata = {
            "nature": "2",
            "type[]": "1",
            "price": "",
            "age": "",
            "tenant_min": "",
            "tenant_max": "",
            "rent_type": "",
            "currency": "EUR",
            "homepage": "",
        }
        api_url = "https://www.rouxel-immo.fr/fr/recherche/"
        yield FormRequest(
            url=api_url,
            callback=self.parse,
            formdata=formdata,
            dont_filter=True,
            meta={
                "property_type":"apartment",
            })

    p_info = True
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//li[@class='ad']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    
        if self.p_info:
            self.p_info = False
            formdata = {
                "nature": "2",
                "type[]": "2",
                "price": "",
                "age": "",
                "tenant_min": "",
                "tenant_max": "",
                "rent_type": "",
                "currency": "EUR",
                "homepage": "",
            }
            api_url = "https://www.rouxel-immo.fr/fr/recherche/"
            yield FormRequest(
                url=api_url,
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":"house",
                })

        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Rouxel_Immo_PySpider_france", input_type="VALUE")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])

        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='path']/p/span/text()", input_type="F_XPATH", replace_list={"Appartement":"", "Studio":""})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='path']/p/span/text()", input_type="F_XPATH", replace_list={"Appartement":"", "Studio":""})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h2[@class='price']/text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(.,'Surface')]/span/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[contains(.,'Etage')]/span/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'Salle')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Ascenseur')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges')]/span/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//img[contains(@alt,'Consom')]/@src", input_type="F_XPATH", get_num=True, split_list={"/":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[@class='comment']//text()[contains(.,'Ref')]", input_type="F_XPATH", split_list={".":1})
        
        room_count = response.xpath("//li[contains(.,'Chambre')]/text() or //li[contains(.,'pièce')]/text() or //span[contains(.,'pièce')]").re_first(r"\d+")
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[contains(.,'Pièce')]/text()").get()
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        deposit = response.xpath("//li[contains(.,'de garantie')]/span/text()").get()
        if deposit:
            deposit = deposit.split(" ")[0].replace(",",".")
            item_loader.add_value("deposit", int(float(deposit)))
        
        desc = " ".join(response.xpath("//p[@class='comment']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'resizePicture')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//aside/h4/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//aside//a[contains(@href,'tel')]/text()", input_type="F_XPATH", replace_list={"\u00a0":"", "+":""})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="rouxel-immo@wanadoo.fr", input_type="VALUE")   
        
        yield item_loader.load_item()
