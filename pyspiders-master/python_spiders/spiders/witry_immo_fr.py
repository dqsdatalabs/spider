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
    name = 'witry_immo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Witry_Immo_PySpider_france"
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
                "https://www.witry-immo.fr/recherche/",
                callback=self.jump,
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
            "data[Search][prixmin]": "0",
            "data[Search][prixmax]": "1480",
        }
        api_url = "https://www.witry-immo.fr/recherche/"
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
        for item in response.xpath("//meta[@itemprop='url']"):
            follow_url = response.urljoin(item.xpath("./@content").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.witry-immo.fr/recherche/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"page":page+1, "property_type":response.meta["property_type"], "current_index":current_index})
        elif current_index < 3:
            yield Request(
                "https://www.witry-immo.fr/recherche/",
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
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta["property_type"])

        title = "".join(response.xpath("//div[@class='bienTitle']/h2//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p/span[contains(.,'Loyer')]/following-sibling::span/text()", input_type="M_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p/span[contains(.,'Ville')]/following-sibling::span/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p/span[contains(.,'Ville')]/following-sibling::span/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//p/span[contains(.,'Code')]/following-sibling::span/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//p/span[contains(.,'Etage')]/following-sibling::span/text()", input_type="M_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//p[@itemprop='description']//text()", input_type="M_XPATH", split_list={"Libre":1})
        
        square_meters = response.xpath("//p/span[contains(.,'habitable')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        if response.xpath("//p/span[contains(.,'chambre')]/following-sibling::span/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p/span[contains(.,'chambre')]/following-sibling::span/text()", input_type="M_XPATH", get_num=True)
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p/span[contains(.,'pièce')]/following-sibling::span/text()", input_type="M_XPATH", get_num=True)
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p/span[contains(.,'salle')]/following-sibling::span/text()", input_type="M_XPATH", get_num=True)
        
        desc = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//p/span[contains(.,'parking')]/following-sibling::span/text()[.!='0']", input_type="M_XPATH", tf_item=True)
        
        furnished = response.xpath("//p/span[contains(.,'Meublé')]/following-sibling::span/text()").get()
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
        
        elevator = response.xpath("//p/span[contains(.,'Ascenseur')]/following-sibling::span/text()").get()
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
        
        balcony = response.xpath("//p/span[contains(.,'Balcon')]/following-sibling::span/text()").get()
        if balcony:
            if "oui" in balcony.lower():
                item_loader.add_value("balcony", True)
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
        
        terrace = response.xpath("//p/span[contains(.,'Terrasse')]/following-sibling::span/text()").get()
        if terrace:
            if "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False) 
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p/span[contains(.,'garantie')]/following-sibling::span/text()", input_type="M_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//p/span[contains(.,'Charge')]/following-sibling::span/text()", input_type="M_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'imageGallery')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[@class='ref']/text()", input_type="F_XPATH", split_list={"Ref":1})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lat :":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lng:":1, "}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="WITRY IMMO", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="03 26 40 23 19", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@witry-immo.fr", input_type="VALUE")

        
        yield item_loader.load_item()
