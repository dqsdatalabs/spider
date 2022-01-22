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
    name = 'ab_immobilier_menton_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "http://www.ab-immobilier-menton.com/votre-recherche/"
    current_index = 0
    other_prop = ["39", "4"]
    other_prop_type = ["house", "studio"]
    def start_requests(self):
        formdata = {
            "data[Search][offredem]": "2",
            "data[Search][idtype][]": "2",
            "data[Search][prixmax]": "",
            "data[Search][piecesmin]": "",
            "data[Search][NO_DOSSIER]": "",
            "data[Search][distance_idvillecode]": "",
            "data[Search][prixmin]": "",
            "data[Search][surfmin]": "",
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
        for item in response.xpath("//a[@class='block-link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        if page == 2 or seen:
            p_url = f"http://www.ab-immobilier-menton.com/votre-recherche/{page}"
            yield Request(p_url, dont_filter=True, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": self.other_prop[self.current_index],
                "data[Search][prixmax]": "",
                "data[Search][piecesmin]": "",
                "data[Search][NO_DOSSIER]": "",
                "data[Search][distance_idvillecode]": "",
                "data[Search][prixmin]": "",
                "data[Search][surfmin]": "",
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
        title = response.xpath("//div[@class='col-md-12']//h2/text()").get()
        if title:
            title = re.sub("\s{2,}", " ", title)
            item_loader.add_value("title", title)
            item_loader.add_value("address", title.split(" - ")[-1])
        room_count = response.xpath("//p[span[contains(.,'chambre')]]/span[2]/text()").get()
        if room_count:    
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//p[span[contains(.,'pièce')]]/span[2]/text()")

        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p[span[contains(.,'Nb de salle d')]]/span[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Ab_Immobilier_Menton_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[@class='ref']//text()", input_type="F_XPATH",split_list={"Ref":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//p[span[.='Code postal']]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p[span[.='Ville']]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//p[span[.='Etage']]/span[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[@itemprop='description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//p[span[contains(.,'Surface habitable (m²)')]]/span[2]/text()", input_type="F_XPATH", get_num=True, split_list={",":0,"m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[span[contains(.,'Loyer CC* / mois')]]/span[2]/text()", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p[span[contains(.,'Dépôt de garantie ')]]/span[2]/text()", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//p[span[contains(.,'Charges locatives ')]]/span[2]/text()", input_type="F_XPATH", get_num=True, split_list={",":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'imageGallery')]/li//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, "lng:":1, "}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//p[span[contains(.,'Nombre de parking') or contains(.,'Nombre de garage')]]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//p[span[contains(.,'Terrasse')]]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//p[span[contains(.,'Meublé')]]/span[2]/text()[.!='Non renseigné']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//p[span[.='Ascenseur']]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//p[span[.='Balcon']]/span[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Ab immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04.93.35.02.91", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="ab.immobilier12@gmail.com", input_type="VALUE")    
      
        yield item_loader.load_item()