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
    name = 'moullin_traffort_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.moullin-traffort.com/resultats.php?page=annonces.php&transac=location&type=appartement&budget_mini=&budget_maxi=&surface_mini=&surface_maxi=&nb_pieces=0&ville=&submit=rechercher",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.moullin-traffort.com/resultats.php?page=annonces.php&transac=location&type=maison&budget_mini=&budget_maxi=&surface_mini=&surface_maxi=&nb_pieces=0&ville=&submit=rechercher",
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
        for item in response.xpath("//a[@class='btn']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Moullin_Traffort_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//td[@colspan='2']//text()[contains(.,'Situation')]", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//td[@colspan='2']//text()[contains(.,'Situation')]", input_type="F_XPATH", split_list={":":1, " ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//td[@colspan='2']//text()[contains(.,'Situation')]", input_type="F_XPATH", split_list={":":1, " ":1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//td/h3[contains(.,'DESCRIPTION')]/../text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//td[@colspan='2']//text()[contains(.,'habitable')]", input_type="F_XPATH", get_num=True, split_list={":":1, "m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//td[@colspan='2']//text()[contains(.,'Loyer :')]", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//td[@align='right'][contains(.,'Ref')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//td/h3[contains(.,'DESCRIPTION')]/../text()[contains(.,'disponible le')]", input_type="F_XPATH", split_list={"disponible le":1,".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//a[@data-fancybox-group='gallery']//@href", input_type="M_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'center:')]/text()", input_type="F_XPATH", split_list={"center:[":1,",":0})
        # ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'center:')]/text()", input_type="F_XPATH", split_list={"center:[":1,",":1, "]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//td[@colspan='2']//text()[contains(.,'Charges :')]", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Cabinet Moullin Traffort", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="05 62 73 55 85", input_type="VALUE")
        
        features = "".join(response.xpath("//td/h3[contains(.,'INFORMATIONS')]/..//td//text()").getall())
        if features:
            if "chambre" in features:
                room_count = features.split("chambre(s) :")[1].strip().split(" ")[0]
                item_loader.add_value("room_count", room_count)
            else:
                ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//td[@colspan='2']//text()[contains(.,'pièce(s)')]", input_type="F_XPATH", get_num=True, split_list={":":1})
                
            if "salle(s) de bain :" in features:
                item_loader.add_value("bathroom_count", features.split("salle(s) de bain :")[1].strip().split(" ")[0])
            if "Parking" in features:
                item_loader.add_value("parking", True)
            if "Ascenseur" in features:
                item_loader.add_value("elevator", True)
            if "terrasse" in features.lower():
                item_loader.add_value("terrace", True)
            if "balcon" in features.lower():
                item_loader.add_value("balcony", True)
            if "Etage :" in features:
                item_loader.add_value("floor", features.split("Etage :")[1].strip().split(" ")[0])
            if "Meublé" in features:
                item_loader.add_value("furnished", True)
        
        energy_label = response.xpath("//img/@alt[contains(.,'DPE')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[1].strip()    )
        
        yield item_loader.load_item()