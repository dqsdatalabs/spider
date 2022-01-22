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
    name = 'agence_immovictorhugo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):

        start_urls = [
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
            payload = {
                "data[Search][offredem]": "2",
                "data[Search][idtype]": r_type,
                "data[Search][idvillecode]": "void",
            }

            yield FormRequest(url="http://www.agence-immovictorhugo.com/recherche/",
                                callback=self.parse,
                                formdata=payload,
                                dont_filter=True,
                                meta={'property_type': url.get('property_type')})
            
    # 1. FOLLOWING
    def parse(self, response): 

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//a[contains(.,'Voir le bien')]/@href").extract():
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
                'Referer': f'http://www.agence-immovictorhugo.com/recherche/{page}',
                'Accept-Language': 'tr,en;q=0.9',
            }
            yield Request(
                f'http://www.agence-immovictorhugo.com/recherche/{page}', 
                callback=self.parse, 
                meta={"property_type" : response.meta.get("property_type"), "page": page + 1})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agence_Immovictorhugo_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p/span[contains(.,'Ville')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//p/span[contains(.,'Code')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p/span[contains(.,'Ville')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="normalize-space(//div[@class='bienTitle']/h2/text())", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[@itemprop='description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//p/span[contains(.,'habitable')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ",":0})
        
        if response.xpath("//p/span[contains(.,'chambre')]/following-sibling::span/text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p/span[contains(.,'chambre')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p/span[contains(.,'pièce')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p/span[contains(.,'salle')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p/span[contains(.,'Loyer')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p/span[contains(.,'Dépôt')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[@class='ref']/text()", input_type="F_XPATH", split_list={"Ref":1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'imageGallery')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lat :":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lng:":1, "}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//p/span[contains(.,'Etage')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//p/span[contains(.,'Dont')]/following-sibling::span/text()[not(contains(.,'Non'))]", input_type="F_XPATH", get_num=True, split_list={" ":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//p/span[contains(.,'garage')]/following-sibling::span/text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//p/span[contains(.,'Balcon')]/following-sibling::span/text()[contains(.,'OUI') or contains(.,'Oui') or contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//p/span[contains(.,'Meublé')]/following-sibling::span/text()[contains(.,'OUI') or contains(.,'Oui') or contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//p/span[contains(.,'Ascenseur')]/following-sibling::span/text()[contains(.,'OUI') or contains(.,'Oui') or contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//p/span[contains(.,'Terrasse')]/following-sibling::span/text()[contains(.,'OUI') or contains(.,'Oui') or contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="IMMOBILIERE VICTOR HUGO", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 76 473 500", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="cr@immovictorhugo.com", input_type="VALUE")

        yield item_loader.load_item()
