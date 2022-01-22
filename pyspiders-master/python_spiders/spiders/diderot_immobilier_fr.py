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
    name = 'diderot_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.diderot-immobilier.fr/catalog/advanced_search_result.php?action=update_search&search_id=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=UNIQUE&C_27=1&C_27_REPLACE=1&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_30_search=COMPRIS&C_30_MIN=0&C_30_type=NUMBER&C_30_MAX=",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//img[@class='w100']/../@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[@class='page_suivante']/@href").get()
        if next_page:
            p_url = response.urljoin(next_page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_id", response.url.split("_")[-1].split("/")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Diderot_Immobilier_PySpider_france", input_type="VALUE")
        #ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[contains(text(),'Secteur')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='description']/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[contains(text(),'Surface')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(text(),'Nombre pièces')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[contains(text(),\"Salle(s) d'eau\")]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='prix loyer']//span[@class='alur_loyer_price']/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[contains(text(),'Dépôt de Garantie')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div[contains(text(),'Provision sur charges')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[contains(text(),'Meublé')]/following-sibling::div/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@class='coordonnees']/h5/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='coordonnees']//text()[contains(.,'Tél')]", input_type="F_XPATH", split_list={":":-1})

        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[contains(text(),'Ascenseur')]/following-sibling::div/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div[.='Etage']/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[contains(text(),'Conso Energ')]/following-sibling::div/text()", input_type="F_XPATH")

        address = response.xpath("//div[contains(text(),'Secteur')]/following-sibling::div/text()").get()
        if address:
            item_loader.add_value("address", address)
        else:
            address = response.xpath('//span[@class="alur_location_ville"]/text()').get()
            if address:
                item_loader.add_value("address", address)       
        
        images = [response.urljoin(x.split("url('")[-1].split("'")[0]) for x in response.xpath("//div[@class='minPhoto']/div/@style").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        parking = response.xpath("//div[contains(text(),'Nombre places parking')]/following-sibling::div/text()").get()
        if parking:
            if int(parking) > 0: item_loader.add_value("parking", True)
            elif int(parking) == 0: item_loader.add_value("parking", False)

        balcony = response.xpath("//div[contains(text(),'Nombre balcons')]/following-sibling::div/text()").get()
        if balcony:
            if int(balcony) > 0: item_loader.add_value("balcony", True)
            elif int(balcony) == 0: item_loader.add_value("balcony", False)

        yield item_loader.load_item()