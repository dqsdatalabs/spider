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
    name = 'immobilier_investim_com'
    execution_type='testing'
    country='france'
    locale='fr'

    custom_settings = { 
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
    }    


    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "2",
            },
            {
                "property_type" : "studio",
                "type" : "4",
            },
        ]
        for item in start_urls:
            formdata = {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": item["type"],
                "data[Search][NO_DOSSIER]": "",
                "data[Search][prixmin]": "",
                "data[Search][prixmax]": "",
                "data[Search][surfmin]": "",
                "data[Search][surfmax]": "",
                "data[Search][piecesmin]": "",
                "data[Search][piecesmax]": "",
                "data[Search][idvillecode]": "void",
                "data[Search][distance_idvillecode]": "",
            }
            api_url = "https://www.immobilier-investim.fr/recherche/"
            yield FormRequest(
                url=api_url,
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                })

    # 1. FOLLOWING
    def parse(self, response):        
        for item in response.xpath("//div/@onclick").extract():
            follow_url = response.urljoin(item.split("'")[1])
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Immobilier_Investim_PySpider_france", input_type="VALUE")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        title = " ".join(response.xpath("//div[contains(@class,'bienTitle')]/h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='prix']//text()", input_type="M_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[@class='ref']//text()", input_type="F_XPATH", split_list={"Ref":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p[@class='data']/span[contains(.,'Ville')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p[@class='data']/span[contains(.,'Ville')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//p[@class='data']/span[contains(.,'Code')]/following-sibling::span/text()", input_type="F_XPATH")
        
        square_meters = response.xpath("//p[@class='data']/span[contains(.,'habitable')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        if response.xpath("//p[@class='data']/span[contains(.,'chambre')]/following-sibling::span/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p[@class='data']/span[contains(.,'chambre')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p[@class='data']/span[contains(.,'pièce')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p[@class='data']/span[contains(.,'salle')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//p[@class='data']/span[contains(.,'Etage')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//p[@class='data']/span[contains(.,'Meublé')]/following-sibling::span/text()[not(contains(.,'Non'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p[@class='data']/span[contains(.,'de garantie')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//p[@class='data']/span[contains(.,'Charge')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//p[@class='data']/span[contains(.,'Ascenseur')]/following-sibling::span/text()[not(contains(.,'NON'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//p[@class='data']/span[contains(.,'Balcon')]/text()", input_type="F_XPATH", tf_item=True)
        
        desc = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'lat :':1, ',':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'lng:':1, '}':0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'imageGallery')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="INVESTIM IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="02 54 78 46 20", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@immobilier-investim.com", input_type="VALUE")
        
        yield item_loader.load_item()