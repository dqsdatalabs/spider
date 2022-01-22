# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'grandnimes_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.grandnimes.com/catalog/advanced_search_result.php?action=update_search&search_id=1692485490482455&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.grandnimes.com/catalog/advanced_search_result.php?action=update_search&search_id=&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='img-product']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//li[contains(@class,'next-link')]/a/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Grandnimes_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//li/div[@class='row']/div[contains(.,'Ville')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//li/div[@class='row']/div[contains(.,'Code')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//li/div[@class='row']/div[contains(.,'Ville')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='product-description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li/div[@class='row']/div[contains(.,'Surface')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ".":0})
        
        if response.xpath("//li/div[@class='row']/div[contains(.,'Chambre')]/following-sibling::div//text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/div[@class='row']/div[contains(.,'Chambre')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/div[@class='row']/div[contains(.,'pièce')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True)
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li/div[@class='row']/div[contains(.,'Salle')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li/div[@class='row']/div[contains(.,'Loyer charg')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True, split_list={"EUR":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li/div[@class='row']/div[contains(.,'Disponibilité')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li/div[@class='row']/div[contains(.,'Garantie')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True, split_list={"EUR":0})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="substring-after(//div[@class='product-ref']/text(),':')", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='item-slider']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li/div[@class='row']/div[contains(.,'Etage')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li/div[@class='row']/div[contains(.,'sur charges')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True, split_list={"EUR":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li/div[@class='row']/div[contains(.,'garage') or contains(.,'parking')]/following-sibling::div//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li/div[@class='row']/div[contains(.,'Balcon') or contains(.,'balcon')]/following-sibling::div//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li/div[@class='row']/div[contains(.,'Meublé')]/following-sibling::div//text()[contains(.,'Oui') or contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li/div[@class='row']/div[contains(.,'Ascenseur')]/following-sibling::div//text()[contains(.,'Oui') or contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li/div[@class='row']/div[contains(.,'terrasse')]/following-sibling::div//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Agence Grand Nîmes", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 66 36 61 61", input_type="VALUE")

        item_loader.add_xpath("energy_label", "//li/div[@class='row']/div[contains(.,'Conso')]/following-sibling::div//text()[not(contains(.,'Vierge'))]")
        
        yield item_loader.load_item()