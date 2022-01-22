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
    name = 'rond_point_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Rond_Point_Immobilier_PySpider_france"

    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "2",
            },
        ]
        for item in start_urls:
            formdata = {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": item["type"],
                "data[Search][prixmax]": "",
            }
            api_url = "https://www.rond-point-immobilier.com/recherche/"
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
        for item in response.xpath("//div[contains(@class,'item__links')]//a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Rond_Point_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[contains(text(),'Réf')]/text()", input_type="F_XPATH", split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//span[contains(.,'Code postal')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[contains(.,'Ville')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'text-block')]/*//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[contains(.,'Surface habitable')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True, split_list={",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'Nombre de chambre')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'Nb de salle de bains')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(.,'Loyer CC* / mois')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(.,'Dépôt de garantie TTC')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'swiper-wrapper')]//a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[contains(.,'Charges locatives')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//span[contains(.,'Etage')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//span[contains(.,'Meublé')]/following-sibling::span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//span[contains(.,'Ascenseur')]/following-sibling::span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//span[contains(.,'Balcon')]/following-sibling::span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//span[contains(.,'Terrasse')]/following-sibling::span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Rond Point Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 91 77 41 42", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="rondpointimmo@orange.fr", input_type="VALUE")

        parking = response.xpath("//span[contains(.,'Nombre de garage')]/following-sibling::span/text()").get()
        if parking:
            if int(parking) > 0: item_loader.add_value("parking", True)
            elif int(parking) == 0: item_loader.add_value("parking", False)
        
        yield item_loader.load_item()
