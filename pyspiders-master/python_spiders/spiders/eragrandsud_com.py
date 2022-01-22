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
import math

class MySpider(Spider):
    name = 'eragrandsud_com'
    execution_type='testing'
    country='france'
    locale='fr'
    # DYNAMIC DATA
    b_url = "https://www.immobilier-muret-labarthe-roques-seysses-era.fr"
    source_name = "Eragrandsud_PySpider_france"
    # --------------

    def start_requests(self):
        headers = {
            "Accept":"*/*",
            "Accept-Encoding":"gzip, deflate, br",
            "Accept-Language":"tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection":"keep-alive",
            "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
            "Host":self.b_url.split('://')[1],
            "Origin":f"{self.b_url}/",
            "X-Requested-With":"XMLHttpRequest",
        }

        start_urls = [
            {"url": f"{self.b_url}/catalog/advanced_search_result_carto.php?action=update_search&ville=&map_polygone=&latlngsearch=&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=1&cfamille_id_tmp=1&cfamille_id_tmp=2&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Vente&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&30min=&C_30_MAX=&C_30_tmp=&check_C_28=Vente&C_33_search=EGAL&C_33_type=NUMBER&C_33_MIN=&C_33=&C_38_search=EGAL&C_38_type=NUMBER&C_38_MIN=&C_38=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30min=&30max=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_47_search=SUPERIEUR&C_47_type=NUMBER&C_46_search=SUPERIEUR&C_46_type=NUMBER&C_41_search=EGAL&C_41_type=NUMBER&C_50_search=SUPERIEUR&C_50_type=NUMBER&C_110_search=EGAL&C_110_type=NUMBER&C_1737_search=EGAL&C_1737_type=NUMBER&C_49_search=SUPERIEUR&C_49_type=NUMBER&C_48_search=SUPERIEUR&C_48_type=NUMBER&keywords=", 
             "property_type" : "apartment", 
              "prop_type" : "1"},

            {"url": f"{self.b_url}/catalog/advanced_search_result_carto.php?action=update_search&ville=&map_polygone=&latlngsearch=&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=2&cfamille_id_tmp=1&cfamille_id_tmp=2&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Vente&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&30min=&C_30_MAX=&C_30_tmp=&check_C_28=Vente&C_33_search=EGAL&C_33_type=NUMBER&C_33_MIN=&C_33=&C_38_search=EGAL&C_38_type=NUMBER&C_38_MIN=&C_38=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30min=&30max=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_47_search=SUPERIEUR&C_47_type=NUMBER&C_46_search=SUPERIEUR&C_46_type=NUMBER&C_41_search=EGAL&C_41_type=NUMBER&C_50_search=SUPERIEUR&C_50_type=NUMBER&C_110_search=EGAL&C_110_type=NUMBER&C_1737_search=EGAL&C_1737_type=NUMBER&C_49_search=SUPERIEUR&C_49_type=NUMBER&C_48_search=SUPERIEUR&C_48_type=NUMBER&keywords=", 
             "property_type" : "house", 
              "prop_type" : "2"}
        ]

        for url in start_urls:
            data = {
                "jquery_aa_afunc":"call",
                "remote_function":"get_products_search_ajax_perso",
                "params[0][map_polygone]":"-0.3130681184533001,43.340570488571956/-0.3130681184533001,43.26286582173947/-0.3978930340614949,43.26286582173947/-0.3978930340614949,43.340570488571956",
                "params[0][action]":"",
                "params[0][C_28_search]":"EGAL",
                "params[0][C_28_type]":"UNIQUE",
                "params[0][C_28]":"Vente",
                "params[0][cfamille_id_search]":"EGAL",
                "params[0][cfamille_id_type]":"TEXT",
                "params[0][cfamille_id]": "{}".format(url.get('prop_type')),
                "params[0][C_30_search]":"COMPRIS",
                "params[0][C_30_type]":"NUMBER",
                "params[0][C_30_MIN]":"",
                "params[0][C_30_MAX]":"",
                "params[0][C_47_search]":"SUPERIEUR",
                "params[0][C_47_type]":"NUMBER",
                "params[0][C_47]":"",
                "params[0][C_46_search]":"SUPERIEUR",
                "params[0][C_46_type]":"NUMBER",
                "params[0][C_46]":"",
                "params[0][C_41_search]":"SUPERIEUR",
                "params[0][C_41_type]":"NUMBER",
                "params[0][C_41]":"",
                "params[0][C_50_search]":"SUPERIEUR",
                "params[0][C_50_type]":"NUMBER",
                "params[0][C_50]":"",
                "params[0][C_110_search]":"EGAL",
                "params[0][C_110_type]":"FLAG",
                "params[0][C_110]":"",
                "params[0][C_1737_search]":"EGAL",
                "params[0][C_1737_type]":"FLAG",
                "params[0][C_1737]":"",
                "params[0][C_49_search]":"SUPERIEUR",
                "params[0][C_49_type]":"NUMBER",
                "params[0][C_49]":"",
                "params[0][C_48_search]":"SUPERIEUR",
                "params[0][C_48_type]":"NUMBER",
                "params[0][C_48]":"",
                "params[0][C_34_search]":"COMPRIS",
                "params[0][C_34_type]":"NUMBER",
                "params[0][C_34_MIN]":"",
                "params[0][admin_id]":"",
                "params[]": "true",
                "params[]": "400",
                "params[]":"map_carto",
                "params[]": "../templates/era-agences/catalog/images/marker2.png",
                "params[]":"",
                "params[]":"../templates/era-agences/catalog/images/marker.png",
                "params[]":"false",
                "params[]":"../templates/era-agences/catalog/images/marker_cluster.png",
            }
            yield Request(url=url.get('url'), callback=self.parse, method="POST", headers=headers, body=json.dumps(data), meta={'property_type': url.get("property_type")})
    
    # 1. FOLLOWING
    def parse(self, response):

        seen = False
        for item in response.xpath("//div[@id='listing_bien']/div//h2/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get("property_type")})
            seen = True
        
        pagination = response.xpath("//ul[@class='pagination']/li/a[@class='page_suivante']/@href").get()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse, meta={'property_type': response.meta.get("property_type")})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value=self.source_name, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='criteres-list']/ul/li/div[contains(.,'Référence')]//following-sibling::div/text()", input_type="F_XPATH")
        split_list = {}
        if '\u00e8me' in response.xpath("//h1/text()").get(): split_list = {" ":-2}
        else: split_list = {" ":-1}
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH", split_list=split_list)
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list=split_list)
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'description principale')]/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@class='criteres-list']/ul/li/div[contains(.,'Surface')]//following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='criteres-list']/ul/li/div[contains(.,'pièces')]//following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[contains(text(),'Salle de bain')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@id='div-infos-bien']//div[@class='prix loyer']/span[1]/text()", input_type="F_XPATH", get_num=True, split_list={".":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//img[contains(@src,'reglettes-dpe-ges/dpe')]/@src", input_type="F_XPATH", split_list={"-":-1, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//text()[contains(.,'Dépôt de garantie') or contains(.,'Dépot de garantie')]", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='flex_slider_bien']/ul/li/a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'myLatlng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'myLatlng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="normalize-space(//span[contains(text(),'dont charges')]/text())", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//p[@class='name']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='contact_nego_boutons_return']/a/font//text()", input_type="M_XPATH", split_list={" ":0})
        
        elevator = response.xpath("//div[@class='criteres-list']/ul/li/div[contains(.,'Ascenseur')]/following-sibling::div/text()").get()
        if elevator:
            if int(elevator) > 0: item_loader.add_value("elevator", True)
            elif int(elevator) == 0: item_loader.add_value("elevator", False)
        
        parking = response.xpath("//div[@class='criteres-list']/ul/li/div[contains(.,'Garage') or contains(.,'Parking')]/following-sibling::div/text()").get()
        if parking:
            if int(parking) > 0: item_loader.add_value("parking", True)
            elif int(parking) == 0: item_loader.add_value("parking", False)
        
        balcony = response.xpath("//div[@class='criteres-list']/ul/li/div[contains(.,'Balcon')]/following-sibling::div/text()").get()
        if balcony:
            if int(balcony) > 0: item_loader.add_value("balcony", True)
            elif int(balcony) == 0: item_loader.add_value("balcony", False)
        
        terrace = response.xpath("//div[@class='criteres-list']/ul/li/div[contains(.,'Terrasse')]/following-sibling::div/text()").get()
        if terrace:
            if int(terrace) > 0: item_loader.add_value("terrace", True)
            elif int(terrace) == 0: item_loader.add_value("terrace", False)
        
        swimming_pool = response.xpath("//div[@class='criteres-list']/ul/li/div[contains(.,'Piscine')]/following-sibling::div/text()").get()
        if swimming_pool:
            if int(swimming_pool) > 0: item_loader.add_value("swimming_pool", True)
            elif int(swimming_pool) == 0: item_loader.add_value("swimming_pool", False)

        yield item_loader.load_item()