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
    name = 'agenceprincipaleepone_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agenceprincipaleepone.com/catalog/advanced_search_result.php?action=update_search&search_id=1689573629403035&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_30_search=COMPRIS&C_30_type=NUMBER&C_33_search=COMPRIS&C_33_type=NUMBER&C_33=&C_33_MAX=&C_34_search=COMPRIS&C_34_type=NUMBER&C_28_tmp=Location&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_34_MIN=&C_34_MAX=&C_30_MIN=&C_30_MAX=&C_33_MIN=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agenceprincipaleepone.com/catalog/advanced_search_result.php?C_34_MAX=&C_41_search=EGAL&C_41_type=FLAG&C_41=&C_41_temp=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MIN=&C_36_MAX=&C_38_search=EGAL&C_38_type=NUMBER&C_38=&C_38_tmp=&action=update_search&search_id=&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_30_search=COMPRIS&C_30_type=NUMBER&C_33_search=CONTIENT&C_33_type=TEXT&C_33=&C_34_search=COMPRIS&C_34_type=NUMBER&C_27_tmp=2&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_MIN=&C_34_MIN=&C_30_MAX=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='listing-cell']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agenceprincipaleepone_PySpider_france", input_type="VALUE")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='alur_loyer_price']/text()", input_type="F_XPATH", get_num=True, split_list={"Loyer":1, "€":0}, replace_list={"\xa0":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//h2//text()[contains(.,'Ref')]", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//li/div/div[contains(.,'Ville')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//li/div/div[contains(.,'Ville')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//li/div/div[contains(.,'Code')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/div/div[contains(.,'Chambre')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li/div/div[contains(.,'Salle')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li/div/div[contains(.,'Meublé')]/following-sibling::div//text()[contains(.,'oui') or contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li/div/div[contains(.,'Disponibilité')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li/div/div[contains(.,'balcon')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li/div/div[contains(.,'terrasse')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li/div/div[contains(.,'Ascenseur')]/following-sibling::div//text()[contains(.,'oui') or contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li/div/div[contains(.,'Stationnement')]/following-sibling::div//text()[contains(.,'Garage') or contains(.,'garage')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//li/div/div[contains(.,'Conso Energ')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[contains(@class,'charges')]/text()", input_type="F_XPATH", split_list={":":1, "€":0})
        
        square_meters = response.xpath("//span[contains(@class,'surface')]/text()").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].split("m²")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        desc = " ".join(response.xpath("//div[@class='product-desc']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "de garantie :" in desc:
            deposit = desc.split("de garantie :")[1].split("\u20ac")[0].strip()
            item_loader.add_value("deposit", int(float(deposit.replace(",","."))))
        
        if "\u00e9tage" in desc:
            floor = desc.split("\u00e9tage")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='slide']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@class='title-bloc']/p/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='tel-manufacturer']//p/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="epone@agenceprincipale.com", input_type="VALUE")

        if not item_loader.get_collected_values("deposit"):
            ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(text(),'Dépôt de garantie:')]/text()", input_type="F_XPATH", get_num=True, split_list={".": 0})

        yield item_loader.load_item()