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
import dateparser
from python_spiders.helper import ItemClear
class MySpider(Spider):
    name = 'msgimmo13_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):

        start_urls = [
            {
                "type" : 2,
                "property_type" : "apartment"
            },          
        ] #LEVEL-1

        for url in start_urls:
            r_type = str(url.get("type"))
            payload = {
                "data[Search][offredem]": "2",
                "data[Search][idtype]": r_type,
            }

            yield FormRequest(url="https://www.msgimmo13.com/recherche/",
                                callback=self.parse,
                                formdata=payload,
                                dont_filter=True,
                                meta={'property_type': url.get('property_type')})
            
    # 1. FOLLOWING
    def parse(self, response): 

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//a[@itemprop='url']/@href").extract():
            seen = True
            yield Request(
                response.urljoin(item), 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        
        if page == 2 or seen:
            headers = {
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Referer': f'https://www.msgimmo13.com/recherche/{page}',
                'Accept-Language': 'tr,en;q=0.9',
            }
            yield Request(
                f'https://www.msgimmo13.com/recherche/{page}', 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type"), "page": page + 1})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        if response.url == "https://www.msgimmo13.com/recherche/2":
            return
        
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Msgimmo13_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[@class='ref']/text()", input_type="F_XPATH",split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[contains(@class,'containerGroupDetailInfo')]/h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//li[text()='Ville : ']/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//li[text()='Code postal : ']/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//li[text()='Ville : ']/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[text()='Etage : ']/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='offreContent bxd6 bxt12']/p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[text()='Surface habitable (m²) : ']/span/text()", input_type="F_XPATH", get_num=True, split_list={",":0,"m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li[contains(.,'Loyer CC* / mois')]/span/text()", input_type="F_XPATH", get_num=True, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'Dépôt de garantie')]/span/text()", input_type="F_XPATH", get_num=True, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges locatives')]/span/text()", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='offreContent bxd6 bxt12']/p//text()[contains(.,'Dispo ')]", input_type="F_XPATH", replace_list={" le ":""},split_list={"Dispo ":-1,":":0,"-":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='module_Slider_Content']/ul/li//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, "lng:":1, "}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[text()='Terrasse : ']/span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[text()='Meublé : ']/span/text()[.!='Non renseigné']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[text()='Ascenseur : ']/span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[text()='Balcon : ']/span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[text()='Nombre de garage : ']/span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="MSG Immo 13", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04.91.118.118", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="msg.immo13@wanadoo.fr", input_type="VALUE")    
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'Nb de salle d')]/span/text()", input_type="F_XPATH", get_num=True)

        room_count = response.xpath("//li[text()='Nombre de chambre(s) : ']/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//li[text()='Nombre de pièces : ']/span/text()")
        yield item_loader.load_item()
