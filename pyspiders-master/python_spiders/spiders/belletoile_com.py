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
import re

class MySpider(Spider):
    name = 'belletoile_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.belletoile.com/catalog/advanced_search_result_carto.php?action=update_search&search_id=1691220171562033&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_211_search=EGAL&C_211_type=UNIQUE&C_211=&C_27_REPLACE=1&C_27_search=EGAL&C_27_type=UNIQUE&C_27=1&C_34_search=SUPERIEUR&C_34_type=NUMBER&C_34_MIN=&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&map_polygone=&C_65_REPLACE=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.belletoile.com/catalog/advanced_search_result_carto.php?action=update_search&search_id=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_211_search=EGAL&C_211_type=UNIQUE&C_211=&C_27_REPLACE=2&C_27_search=EGAL&C_27_type=UNIQUE&C_27=2&C_34_search=SUPERIEUR&C_34_type=NUMBER&C_34_MIN=&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&map_polygone=&C_65_REPLACE=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=",
                ],
                "property_type" : "house"
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='titreBien']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[@class='page_suivante']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={
                    "property_type":response.meta["property_type"],
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?search_id")[0])
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Belletoile_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[@itemprop='name'][contains(.,'Ref')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="normalize-space(//div[contains(@class,'display-cell')][contains(.,'m²')]/text())", input_type="F_XPATH", get_num=True, split_list={"m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="normalize-space(//div[contains(@class,'display-cell')][contains(.,'Pièce')]/text())", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.split("-")[0].strip().split(" ")[-1])
            item_loader.add_value("city", address.split("-")[0].strip().split(" ")[-1])
        
        rent = response.xpath("normalize-space(//span[contains(@class,'alur_loyer_price')]/text())").get()
        if rent:
            price = rent.replace("\u00a0","").replace("\u20ac"," ").split(" ")[1]
            item_loader.add_value("rent", int(float(price)))
        
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="normalize-space(//span[contains(@class,'alur_location_charges')]/text())", input_type="F_XPATH", get_num=True, split_list={"€":0, ":":1})
        
        deposit = response.xpath("normalize-space(//span[contains(@class,'alur_location_depot')]/text())").get()
        if deposit:
            deposit = deposit.replace("\u00a0","").replace("\u20ac"," ").split(":")[1].split("€")[0].strip()
            item_loader.add_value("deposit", int(float(deposit)))
            
        desc = " ".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

            if "Dépôt de garantie:" in desc and not item_loader.get_collected_values("deposit"):
                deposit = desc.split("Dépôt de garantie:")[1].split("€")[0].strip()
                if deposit:
                    item_loader.add_value("deposit", deposit)
        
        zipcode = response.xpath("//span[contains(@class,'alur_location_ville')]//text()").get()
        if zipcode:
            zipcode = zipcode.split(" ")[0]
            if zipcode and not zipcode.isalpha():
                item_loader.add_value("zipcode", zipcode)

        
        images = [response.urljoin(x.split("url('")[1].split("'")[0]) for x in response.xpath("//div[contains(@id,'diapoDetail')]//@style[contains(.,'background:url(')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        energy_label = response.xpath("//img/@src[contains(.,'DPE_')]").get()
        if energy_label:
            energy_label = energy_label.split("DPE_")[1].split("_")[0]
            if "vierge" not in energy_label:
                item_loader.add_value("energy_label", energy_label)
        
        if "\u00e9tage" in desc:
            floor = desc.split("\u00e9tage")[0].strip().split(" ")[-1].replace("ème","").replace("er","")
            if floor.replace("nd","").isdigit() or "second" in floor:
                item_loader.add_value("floor", floor)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Belletoile Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01.43.68.27.00", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@belletoile.fr", input_type="VALUE")

        yield item_loader.load_item()