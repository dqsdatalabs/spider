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
    name = 'agence_mercure_cannes_com'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {"HTTPCACHE_ENABLED":False}

    def start_requests(self):
        url = "https://www.agence-mercure-cannes.com/recherche/"
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Origin': 'https://www.agence-mercure-cannes.com',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Referer': 'https://www.agence-mercure-cannes.com/recherche/',
            'Accept-Language': 'tr,en;q=0.9',
        }
        start_urls = [
            {
                "formdata" : {
                        'data[Search][offredem]': '2',
                        'data[Search][idtype]': '2',
                        'data[Search][idvillecode]': 'void',
                    },
                "property_type" : "apartment",
            },
            {
                "formdata" : {
                        'data[Search][offredem]': '2',
                        'data[Search][idtype]': '4',
                        'data[Search][idvillecode]': 'void',
                    },
                "property_type" : "studio",
            },
        ]
        for item in start_urls:
            yield FormRequest(url, formdata=item["formdata"], headers=headers, dont_filter=True, callback=self.parse, meta={'property_type': item["property_type"]})

    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//span[contains(.,'Voir le bien')]/../@onclick").getall():
            seen = True
            yield Request(response.urljoin(item.split("href='")[-1].split("'")[0].strip()), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 2 or seen:
            headers = {
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Referer': f'https://www.agence-mercure-cannes.com/recherche/{page - 1}',
                'Accept-Language': 'tr,en;q=0.9',
            }
            yield Request(f"https://www.agence-mercure-cannes.com/recherche/{page}", 
                            callback=self.parse, 
                            headers=headers, 
                            dont_filter=True,
                            meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//div[@class='bienTitle themTitle']//h1/text()").get()
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))
        room_count = response.xpath("//p[span[contains(.,'chambre')]]/span[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)    
        else:
            room_count = response.xpath("//p[span[contains(.,'de pièce')]]/span[2]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count) 
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agence_Mercure_Cannes_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[@class='ref']//text()", input_type="F_XPATH",split_list={"Ref":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p[span[.='Ville']]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//p[span[.='Code postal']]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p[span[.='Ville']]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//p[span[.='Etage']]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[@itemprop='description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//p[span[contains(.,'Surface habitable (m²)')]]/span[2]/text()", input_type="F_XPATH", get_num=True, split_list={",":0,"m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p[span[contains(.,'Nb de salle d')]]/span[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[span[contains(.,'Loyer CC* / mois')]]/span[2]/text()", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p[span[contains(.,'Dépôt de garantie ')]]/span[2]/text()[not(contains(.,'Non'))]", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//p[span[contains(.,'Charges locatives ')]]/span[2]/text()", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'imageGallery')]/li//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, "lng:":1, "}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//p[span[contains(.,'Nombre de parking') or contains(.,'Nombre de garage')]]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//p[span[contains(.,'Terrasse')]]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//p[span[contains(.,'Meublé')]]/span[2]/text()[not(contains(.,'Non rense'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//p[span[.='Ascenseur']]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//p[span[.='Balcon']]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Agence Mercure", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 93 39 08 06", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="agencemercure@wanadoo.fr", input_type="VALUE")    
      
        yield item_loader.load_item()