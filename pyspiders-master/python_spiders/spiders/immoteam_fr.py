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
    name = 'immoteam_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "http://immoteam.fr/recherche/"
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
        for item in response.xpath("//button[contains(.,'Détails')]/@data-url").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        if page == 2 or seen:
            p_url = f"http://immoteam.fr/recherche/{page}"
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
    
        room_count = response.xpath("//li[contains(.,'Nombre de chambre(s) :')]/text()").get()
        if room_count:    
            item_loader.add_value("room_count", room_count.split(":")[-1])
        else:
            room_count = response.xpath("//li[contains(.,'Nombre de pièce')]/text()").get()
            if room_count:    
                item_loader.add_value("room_count", room_count.split(":")[-1])
                
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'Nb de salle de bains :')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Immoteam_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[@class='ref']//text()", input_type="F_XPATH",split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[contains(.,'Etage :')]/text()", input_type="F_XPATH", split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1[@class='titleBien']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//li[contains(.,'Code postal :')]/text()", input_type="F_XPATH", split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//li[contains(.,'Ville :')]/text()", input_type="F_XPATH", split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//li[contains(.,'Ville :')]/text()", input_type="F_XPATH", split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='offreContent']/p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(.,'Surface habitable')]/text()", input_type="F_XPATH", get_num=True, split_list={":":-1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li[contains(.,'Loyer CC* / mois ')]/text()", input_type="F_XPATH", get_num=True, replace_list={" ":""}, split_list={":":-1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'Dépôt de garantie TTC :')]/text()", input_type="F_XPATH", get_num=True, split_list={":":-1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges locatives ')]/text()", input_type="F_XPATH", get_num=True, split_list={":":-1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='slider_Mdl']/li//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Meublé :')]/text()[not(contains(.,'Non rense'))]", input_type="F_XPATH", tf_item=True, split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon :')]/text()", input_type="F_XPATH", tf_item=True, split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Terrasse :')]/text()", input_type="F_XPATH", tf_item=True, split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Nombre de garage :')]/text()", input_type="F_XPATH", tf_item=True, split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Ascenseur : ')]/text()", input_type="F_XPATH", tf_item=True, split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrasse :')]/text()", input_type="F_XPATH", tf_item=True, split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'center: { lat :')]/text()", input_type="F_XPATH", split_list={"center: { lat :":1, "lng:":1, "}":0})
        landlord_name = response.xpath("//div[@class='commercialcoord']/p[1]//text()[normalize-space()]").get()
        if landlord_name:    
            item_loader.add_value("landlord_name",landlord_name)
            if response.xpath("//div[@class='commercialcoord']/p[2]//text()[not(contains(.,'@'))]").get():
                item_loader.add_xpath("landlord_phone", "//div[@class='commercialcoord']/p[2]//text()[not(contains(.,'@'))]")
            else:
                item_loader.add_xpath("landlord_phone", "substring-after(//a[@class='dispPhoneAgency']/@href,':')")
            item_loader.add_xpath("landlord_email","//div[@class='commercialcoord']/p/a[contains(@href,'mail')]/text()")
        else:
            landlord_name = response.xpath("//div[@class='formContGood']//h2/text()").get()
            if landlord_name:    
                item_loader.add_value("landlord_name",landlord_name)
            item_loader.add_xpath("landlord_phone","//div[@class='formContGood']//a/span/text()")
            item_loader.add_xpath("landlord_email","//input[@name='data[Contact][to]']/@value")


        yield item_loader.load_item()