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
    name = 'cabinetnardi_com'
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
    def start_requests(self):
        yield Request(
                "https://cabinetnardi.com/recherche/",
                callback=self.jump,
                dont_filter=True,
            )
    
    def jump(self, response):
        request_index = response.meta.get("current_index", 0)
        request_list = ["2", "1", "18", "4"]
        request_list_type = ["apartment", "house", "apartment", "studio"]

        formdata = {
            "data[Search][offredem]": "2",
            "data[Search][idtype]": request_list[request_index],
            "data[Search][surfmin]": "",
            "data[Search][surfmax]": "",
            "data[Search][pieces]": "void",
            "data[Search][idvillecode]": "void",
            "data[Search][NO_DOSSIER]": "",
            "data[Search][distance_idvillecode]": "",
            "data[Search][prixmin]": "",
            "data[Search][prixmax]": "",
        }
        api_url = "https://cabinetnardi.com/recherche/"
        yield FormRequest(
            url=api_url,
            callback=self.parse,
            formdata=formdata,
            dont_filter=True,
            meta={
                "property_type": request_list_type[request_index],
                "current_index" : request_index,
            })

    # 1. FOLLOWING
    def parse(self, response):
        current_index = response.meta["current_index"]
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//img[@itemprop='image']/@data-url").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://cabinetnardi.com/recherche/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"page":page+1, "property_type":response.meta["property_type"], "current_index":current_index})
        elif current_index < 3:
            yield Request(
                "https://cabinetnardi.com/recherche/",
                callback=self.jump,
                dont_filter=True,
                meta={
                    "current_index" : current_index + 1
                }
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Cabinetnardi_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//section[contains(@class,'map-infos-city')]//h1//text()", input_type="F_XPATH", split_list={"La ville de":-1} )
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[@class='ref']//text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//ul/li[contains(.,'Code postal')]//text()", input_type="F_XPATH", split_list={":":1} )
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//ul/li[contains(.,'Ville')]//text()", input_type="F_XPATH", split_list={":":1} )
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//ul/li[contains(.,'Surface habitable')]//text()", input_type="F_XPATH", split_list={":":1,",":0} )
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//ul/li[contains(.,'Etage')]//text()", input_type="F_XPATH", split_list={":":1} )
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div/h1[@class='titleBien']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='offreContent']/p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//ul/li[contains(.,'Nombre de chambre')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        room_count = response.xpath("//ul/li[contains(.,'Nombre de chambre')]//text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count.split(":")[-1].strip()) 
        else:
            room_count = response.xpath("//ul/li[contains(.,'Nombre de pièces')]//text()").extract_first()
            if room_count:
                item_loader.add_value("room_count",room_count.split(":")[-1].strip()) 
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//ul/li[contains(.,'Nb de salle d')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//ul/li[contains(.,'Loyer CC* / mois')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//ul/li[contains(.,'Dépôt de garantie')]//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='slider_Mdl']/li//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script/text()[contains(.,'center: { lat :')]", input_type="F_XPATH", split_list={"center: { lat :":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script/text()[contains(.,'center: { lat :')]", input_type="F_XPATH", split_list={"center: { lat :":1,"lng:":1, "}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//ul/li[contains(.,'Charges ')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        parking = response.xpath("//ul/li[contains(.,'Nombre de garage')]//text()").extract_first()    
        if parking:
            if "non" in parking.lower() or "0" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//ul/li[contains(.,'Balcon')]//text()", input_type="F_XPATH", tf_item=True,split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//ul/li[contains(.,'Terrasse')]//text()", input_type="F_XPATH", tf_item=True,split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//ul/li[contains(.,'Meublé :')]//text()", input_type="F_XPATH", tf_item=True,split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//ul/li[contains(.,'Ascenseur')]//text()", input_type="F_XPATH", tf_item=True,split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Cabinet Nardi", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 92 47 71 44", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@cabinetnardi.com", input_type="VALUE")
        
        yield item_loader.load_item()
