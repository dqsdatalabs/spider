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
    name = 'cabinetdelta_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Cabinetdelta_PySpider_france'
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }   
    start_urls = ["https://www.cabinetdelta.com/a-louer/1"]
    
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//a[contains(.,'Détails')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
            seen = True
        
        if page ==2 or seen:
            f_url = f"https://www.cabinetdelta.com/a-louer/{page}"
            yield Request(f_url, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)  
        
        prop_type = response.xpath("//li[contains(.,'Type')]/span/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else: return
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value=self.external_source, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[contains(@itemprop,'productId')]//text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//li[contains(.,'Ville')]//span//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//li[contains(.,'Code')]//text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//li[contains(.,'Ville')]//span//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1[contains(@itemprop,'name')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(.,'Surface habitable')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1,"m":0,",":0})
        if response.xpath("//li[contains(.,'chambre')]//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'chambre')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        elif response.xpath("//li[contains(.,'pièce')]//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'pièce')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'salle')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[contains(@class,'price')]//text()", input_type="F_XPATH", get_num=True, replace_list={"€":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")

        deposit = response.xpath("//li[contains(.,'garantie')]//text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip().replace(" ","")
            if deposit.isdigit():
                item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//li[contains(.,'Charges locatives')]//text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip().split(",")[0]
            if utilities.isdigit():
                item_loader.add_value("utilities", utilities)

        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//img[contains(@class,'slide-img')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'center')]/text()", input_type="F_XPATH", split_list={"lat :":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'center')]/text()", input_type="F_XPATH", split_list={"lng:":1,"}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[contains(.,'Etage')]//text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'parking') or contains(.,'garage')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon')]//text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Meublé')]//text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Ascenseur')]//text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrasse')]//text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="CABINET DELTA IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 94 90 63 44", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@cabinetdelta.com", input_type="VALUE")
  
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "duplex" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None