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
    name = 'agencelatoulousaine_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = { 
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
    }
    formdata_list = [
        {
            "property_type": "apartment",
            "formdata": {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": "2",
            },
        },
        {
            "property_type": "apartment",
            "formdata": {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": "18",
            },
        },
        {
            "property_type": "apartment",
            "formdata": {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": "41",
            },
        },
        {
            "property_type": "studio",
            "formdata": {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": "4",
            },
        },
        {
            "property_type": "house",
            "formdata": {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": "1",
            },
        },
    ]

    def start_requests(self):

        yield FormRequest("http://www.agencelatoulousaine.fr/recherche/",
                        callback=self.parse,
                        formdata=self.formdata_list[0]["formdata"],
                        dont_filter=True,
                        meta={"property_type": self.formdata_list[0]["property_type"], "next_index": 1})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        next_index = response.meta.get("next_index", 1)
        seen = False

        for item in response.xpath("//span[contains(.,'Voir le bien')]/../@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 2 or seen:
            headers = {
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Referer': f"http://www.agencelatoulousaine.fr/recherche/{page}",
                'Accept-Language': 'tr,en;q=0.9',
            }   
            follow_url = f"http://www.agencelatoulousaine.fr/recherche/{page}"
            yield Request(follow_url, 
                        headers=headers, 
                        dont_filter=True, 
                        callback=self.parse, 
                        meta={"page": page + 1, "property_type": response.meta["property_type"], "next_index": next_index})
                      
        elif len(self.formdata_list) > next_index:
            yield FormRequest("http://www.agencelatoulousaine.fr/recherche/",
                            callback=self.parse,
                            formdata=self.formdata_list[next_index]["formdata"],
                            dont_filter=True,
                            meta={"property_type": self.formdata_list[next_index]["property_type"], "page": 2, "next_index": next_index + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//div[@class='col-md-12']//h2/text()").get()
        if title:
            title = re.sub("\s{2,}", " ", title)
            item_loader.add_value("title", title)
            item_loader.add_value("address", title.split(" - ")[-1])
        room_count = response.xpath("//p[span[contains(.,'chambre')]]/span[2]/text()").get()
        if room_count:    
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//p[span[contains(.,'pièce')]]/span[2]/text()")

        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p[span[contains(.,'Nb de salle d')]]/span[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agencelatoulousaine_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='col-md-12']//span[@class='ref']//text()", input_type="F_XPATH",split_list={"Ref":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//p[span[.='Code postal']]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p[span[.='Ville']]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//p[span[.='Etage']]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[@itemprop='description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//p[span[contains(.,'Surface habitable (m²)')]]/span[2]/text()", input_type="F_XPATH", get_num=True, split_list={",":0,"m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[span[contains(.,'Loyer CC* / mois')]]/span[2]/text()", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p[span[contains(.,'Dépôt de garantie ')]]/span[2]/text()[not(contains(.,'Non'))]", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//p[span[contains(.,'Charges locatives ')]]/span[2]/text()[not(contains(.,'Non'))]", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'imageGallery')]/li//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, "lng:":1, "}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//p[span[contains(.,'Nombre de parking') or contains(.,'Nombre de garage')]]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//p[span[contains(.,'Terrasse')]]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//p[span[contains(.,'Meublé')]]/span[2]/text()[not(contains(.,'Non '))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//p[span[.='Ascenseur']]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//p[span[.='Balcon']]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="AGENCE LA TOULOUSAINE", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="05.61.28.08.78", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@agencelatoulousaine.fr", input_type="VALUE")    
      
        yield item_loader.load_item()