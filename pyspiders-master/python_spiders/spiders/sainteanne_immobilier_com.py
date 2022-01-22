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

class MySpider(Spider):
    name = 'sainteanne_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {"HTTPCACHE_ENABLED":False}

    def start_requests(self):
        url = "https://www.sainteanne-immobilier.com/recherche/"
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Origin': 'https://www.sainteanne-immobilier.com',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Referer': 'https://www.sainteanne-immobilier.com/recherche/',
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
                        'data[Search][idtype]': '1',
                        'data[Search][idvillecode]': 'void',
                    },
                "property_type" : "house",
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
                'Referer': f"https://www.sainteanne-immobilier.com/recherche/{page}", 
                'Accept-Language': 'tr,en;q=0.9',
            }
            yield Request(f"https://www.sainteanne-immobilier.com/recherche/{page}", 
                            callback=self.parse, 
                            headers=headers, 
                            dont_filter=True,
                            meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Sainteanne_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[contains(@class,'ref')]//text()", input_type="F_XPATH", split_list={"Ref":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[contains(@id,'dataContent')]//span[contains(.,'Ville')]//following-sibling::span//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[contains(@id,'dataContent')]//span[contains(.,'Code postal')]//following-sibling::span//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[contains(@id,'dataContent')]//span[contains(.,'Ville')]//following-sibling::span//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[contains(@itemprop,'description')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[contains(@id,'dataContent')]//span[contains(.,'Surface habitable')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True, split_list={"m":0,",":0})
        if response.xpath("//div[contains(@id,'dataContent')]//span[contains(.,'chambre')]//following-sibling::span//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@id,'dataContent')]//span[contains(.,'chambre')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//div[contains(@id,'dataContent')]//span[contains(.,'pièce')]//following-sibling::span//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@id,'dataContent')]//span[contains(.,'pièce')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'salle')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(.,'Loyer')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(.,'garantie')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'imageGallery')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//span[contains(.,'Etage')]//following-sibling::span//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[contains(.,'Charges')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//span[contains(.,'Meublé')]//following-sibling::span//text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//span[contains(.,'Ascenseur')]//following-sibling::span//text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//span[contains(.,'Terrasse')]//following-sibling::span//text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lat')]/text()", input_type="F_XPATH", split_list={"lat :":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lat')]/text()", input_type="F_XPATH", split_list={"lng:":1,"}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="SAINT ANNE IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 32 400 107", input_type="VALUE")         
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="sainteanne-immobilier@orange.fr", input_type="VALUE")   

        title = " ".join(response.xpath("//div[contains(@class,'bienTitle')]//h1/text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        yield item_loader.load_item()