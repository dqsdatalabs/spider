# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'chancelimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.chancelimmobilier.fr/immobilier/location-type/appartement-categorie/{}.html",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.chancelimmobilier.fr/immobilier/location-type/maison-categorie/{}.html",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//article"):
            follow_url = response.urljoin(item.xpath("./@data-lien").get())
            lat, lng = item.xpath("./@data-latgps").get(), item.xpath("./@data-longgps").get()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"], "lat":lat, "lng":lng})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta.get("base")
            p_url = base_url.format(page)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1, "base":base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("latitude", response.meta.get('lat'))
        item_loader.add_value("longitude", response.meta.get('lng'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Chancelimmobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//h2[contains(@class,'detail-header-titre')][contains(.,'réf')]//text()", input_type="M_XPATH", split_list={"réf.":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h2[contains(@class,'detail-header-titre')]//text()", input_type="M_XPATH", split_list={"-":1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h2[contains(@class,'detail-header-titre')]//text()", input_type="M_XPATH", split_list={"(":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h2[contains(@class,'detail-header-titre')]//text()", input_type="M_XPATH", split_list={"-":1,"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h2[contains(@class,'detail-header-titre')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[contains(@class,'detail-offre-texte')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//small[contains(.,'m²')]//text()", input_type="F_XPATH", get_num=True, split_list={"m²":0," ":-1})
        if response.xpath("//li[contains(@class,'detail-offre-caracteristique')][contains(.,'chambre')]//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(@class,'detail-offre-caracteristique')][contains(.,'chambre')]//text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        elif response.xpath("//small[contains(.,'pièce')]//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//small[contains(.,'pièce')]//text()", input_type="F_XPATH", get_num=True, split_list={"pièce":0," ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(@class,'detail-offre-caracteristique')][contains(.,'salle')]//text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[contains(@class,'detail-offre-prix')]//text()", input_type="M_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(@class,'detail-offre-caracteristique')][contains(.,'parking') or contains(.,'garage')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(@class,'detail-offre-prestation')][contains(.,'Ascenseur')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(@class,'detail-offre-caracteristique')][contains(.,'terrasse')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@id,'gallery2')]//a//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="CHANCEL IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="33 0 4 93 95 95 00", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@chancelimmobilier.fr", input_type="VALUE")

        charges = "".join(response.xpath("//ul[contains(@class,'detail-offre-liste-frais-charges')]//text()").getall())
        if charges:
            deposit = charges.split("€ de dépôt de garantie")[0].strip().split(" ")[-1].replace("\u00a0","")
            item_loader.add_value("deposit", deposit)
            if "de provisions pour charges" in charges:
                utilities = charges.split("de provisions pour charges")[0].strip().split(" ")[-1]
                if "€" in utilities:
                    item_loader.add_value("utilities", utilities.split("€")[0].strip())
                    
        yield item_loader.load_item()