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
    name = 'francimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "https://www.francimmo.com/recherche/"
    current_index = 0
    other_prop = [ "4", "18","41","1"]
    other_prop_type = ["studio", "apartment","apartment","house"]

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
        formdata = {
            "data[Search][offredem]": "2",
            "data[Search][idtype][]": "2",
            "data[Search][prixmax]": "",
            "data[Search][surfmax]": "",
            "data[Search][piecesmax]": "",
            "data[Search][NO_DOSSIER]": "",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            dont_filter=True,
            formdata=formdata,
            meta={
                "property_type":"apartment",
            }
        )


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'property__content-wrapper')]/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        if page == 2 or seen:
            p_url = f"http://www.francimmo.com/recherche/{page}"
            yield Request(p_url, dont_filter=True, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})
        if self.current_index < len(self.other_prop):
            formdata = { 
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": self.other_prop[self.current_index],
                "data[Search][prixmax]": "",
                "data[Search][surfmax]": "",
                "data[Search][piecesmax]": "",
                "data[Search][NO_DOSSIER]": "",
            }
            yield FormRequest(
                url=self.post_url,
                callback=self.parse,
                dont_filter=True,
                formdata=formdata,
                meta={
                    "property_type":self.other_prop_type[self.current_index],
                }
            )
            self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])  
        prop_type = response.xpath("//li[@class='breadcrumb__item']/a/text()[.='Vente']").get()
        if prop_type:
            return
        title = " ".join(response.xpath("//h1[@class='title__content']/span/text()").getall())
        if title:
            title = re.sub("\s{2,}", " ", title)
            item_loader.add_value("title", title)
        address = response.xpath("//h1[@class='title__content']/span[last()]/text()").get()
        if address: 
            item_loader.add_value("address", address)
        else:
            address = response.xpath("//div[span[.='Ville']]/span[2]/text()").get()
            if address: 
                item_loader.add_value("address", address)
        room_count = response.xpath("//div[span[contains(.,'chambre')]]/span[2]/text()").get()
        if room_count:    
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//div[span[contains(.,'pièce')]]/span[2]/text()")

        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[span[contains(.,'Nb de salle d')]]/span[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Francimmo_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='col-md-12']//span[@class='ref']//text()", input_type="F_XPATH",split_list={"Ref":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[span[.='Code postal']]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[span[.='Ville']]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div[span[.='Etage']]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='detail-1__text']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[span[contains(.,'Surface habitable (m²)')]]/span[2]/text()", input_type="F_XPATH", get_num=True, split_list={",":0,"m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[span[contains(.,'Loyer CC* / mois')]]/span[2]/text()", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[span[contains(.,'Dépôt de garantie ')]]/span[2]/text()[not(contains(.,'Non'))]", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div[span[contains(.,'Charges locatives ')]]/span[2]/text()[not(contains(.,'Non'))]", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'slider-img__swiper-slide')]//img/@data-src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, "lng:":1, "}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[span[contains(.,'Nombre de parking') or contains(.,'Nombre de garage')]]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[span[contains(.,'Terrasse')]]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[span[contains(.,'Meublé')]]/span[2]/text()[not(contains(.,'Non'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[span[.='Ascenseur']]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[span[.='Balcon']]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="FRANCIMMO", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="03 81 64 24 70", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@francimmo.com", input_type="VALUE")    
      
        yield item_loader.load_item()