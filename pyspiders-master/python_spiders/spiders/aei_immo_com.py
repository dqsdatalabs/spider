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
    name = 'aei_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Aei_Immo_PySpider_france"

    custom_settings = {
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
    }
    
    payload = {
        "data[Search][offredem]": "2",
        "data[Search][idtype]": "",
        "data[Search][idvillecode]": "void",
    }
    def start_requests(self):
        start_urls = [
            {
                "type" : 1,
                "property_type" : "house"
            },
            {
                "type" : 2,
                "property_type" : "apartment"
            },
            {
                "type" : 4,
                "property_type" : "studio"
            },
            
        ] #LEVEL-1
        for url in start_urls:
            r_type = str(url.get("type"))
            
            self.payload["data[Search][idtype]"] = r_type
            yield FormRequest(url="http://www.aei-immo.com/recherche/",
                                callback=self.parse,
                                formdata=self.payload,
                                dont_filter=True,
                                meta={'property_type': url.get('property_type'), "type":r_type})
            
    # 1. FOLLOWING
    def parse(self, response): 
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[contains(.,'voir')]/@href").extract():
            seen = True
            yield Request(
                response.urljoin(item), 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type"), "type": response.meta.get('type')},
            )
        
        if page == 2 or seen:
            headers = {
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Referer': f'http://www.aei-immo.com/recherche/{page}',
                'Accept-Language': 'tr,en;q=0.9',
            }
            r_type = response.meta.get('type')
            self.payload["data[Search][idtype]"] = r_type
            yield FormRequest(
                f'http://www.aei-immo.com/recherche/{page}', 
                callback=self.parse,
                headers=headers,
                formdata=self.payload,
                meta={
                    "property_type" : response.meta.get("property_type"),
                    "page": page + 1,
                    "type": response.meta.get('type')
                })
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Aei_Immo_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[contains(@itemprop,'productID')]//text()", input_type="F_XPATH", split_list={"Ref":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//span[contains(.,'Ville')]//following-sibling::span//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//span[contains(.,'Code')]//following-sibling::span//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[contains(.,'Ville')]//following-sibling::span//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[contains(@class,'bienTitle')]//h2//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[contains(@itemprop,'desc')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[contains(.,'Surface habitable')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True, split_list={"m":0,",":0})
        if response.xpath("//span[contains(.,'chambre')]//following-sibling::span//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'chambre')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//span[contains(.,'pièce')]//following-sibling::span//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'pièce')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'salle')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(.,'Loyer')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True, replace_list={"€":""})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(.,'Dépôt de garantie')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True, replace_list={"€":""," ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
 
        utilities = response.xpath("//span[contains(.,'Charge')]//following-sibling::span//text()").get()
        if utilities:
            utilities = utilities.replace("€","").strip()
            if utilities.isdigit():
                item_loader.add_value("utilities", utilities)

        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'imageGallery')]//@data-src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'center')]/text()", input_type="F_XPATH", split_list={"lat :":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'center')]/text()", input_type="F_XPATH", split_list={"lng:":1,"}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//span[contains(.,'Etage')]//following-sibling::span//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//span[contains(.,'Balcon')]//following-sibling::span//text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//span[contains(.,'Meublé')]//following-sibling::span//text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//span[contains(.,'Ascenseur')]//following-sibling::span//text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//span[contains(.,'Terrasse')]//following-sibling::span//text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="AEI IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[contains(@class,'contactdetail')]//span[contains(@class,'tel')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//div[contains(@class,'contactdetail')]//span[contains(@class,'mail')]//text()", input_type="F_XPATH")
  
        yield item_loader.load_item()
