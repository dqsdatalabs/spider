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
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'groupe_c2i_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.groupe-c2i.com/catalog/advanced_search_result_carto.php?action=update_search&search_id=&C_28=Location&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&C_28_search=EGAL&C_28_type=UNIQUE&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=1&cfamille_id_tmp=1&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_30_loc=0&page={}",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.groupe-c2i.com/catalog/advanced_search_result_carto.php?action=update_search&search_id=1692115533580917&C_28=Location&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&C_28_search=EGAL&C_28_type=UNIQUE&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=2&cfamille_id_tmp=2&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_30_loc=0&page={}",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base":item,})

    all_prop = []
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='infosBottom']/@href").getall():
            follow_url = response.urljoin(item).split("?")[0]
            if follow_url not in self.all_prop:
                self.all_prop.append(follow_url)
                seen = True
            else:
                seen = False
                break
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            
        if page == 2 or seen:
            base = response.meta["base"]
            p_url = base.format(page)
            yield Request(
                url=p_url,
                callback=self.parse,
                meta={
                    "property_type":response.meta["property_type"],
                    "page":page+1,
                    "base":base,
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_id", response.url.split("?")[0].split("_")[1].split("/")[0])
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Groupe_C2i_PySpider_france", input_type="VALUE")
        
        if response.xpath("//li/div[contains(.,'Ville')]/div[2]//text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//li/div[contains(.,'Ville')]/div[2]//text()", input_type="F_XPATH")
            ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//li/div[contains(.,'Ville')]/div[2]//text()", input_type="F_XPATH")
        else:
            address = response.xpath("//p[contains(@class,'h1')]/b/text()").get()
            zipcode = address.split(" ")[-2]
            if zipcode.isdigit():
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("address", address.split(" ")[-1])
                item_loader.add_value("city", address.split(" ")[-1])
            elif ":" in address:
                item_loader.add_value("address", address.split(":")[0])
                item_loader.add_value("city", address.split(":")[0])
            elif "-" in address:
                item_loader.add_value("address", address.split(" - ")[0])
                item_loader.add_value("city", address.split(" - ")[0])
            else:
                item_loader.add_value("address", address.split(" ")[0])
                item_loader.add_value("city", address.split(" ")[0])
                
                
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//li/div[contains(.,'Code')]/div[2]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//p[contains(@class,'h1')]/b/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li/div[contains(.,'Surface')]/div[2]//text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ".":0})
        if response.xpath("//li/div[contains(.,'Chambres')]/div[2]//text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/div[contains(.,'Chambres')]/div[2]//text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//li/div[contains(.,'pièces')]/div[2]//text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/div[contains(.,'pièces')]/div[2]//text()", input_type="F_XPATH", get_num=True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li/div[contains(.,'Salle')]/div[2]//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li/div[contains(.,'Loyer charges')]/div[2]//text()", input_type="F_XPATH", get_num=True, split_list={"EUR":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li/div[contains(.,'Disponibilité')]/div[2]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li/div[contains(.,'Garantie')]/div[2]//text()", input_type="F_XPATH", get_num=True, split_list={"EUR":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='photos']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li/div[contains(.,'étages')]/div[2]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li/div[contains(.,'sur charges')]/div[2]//text()", input_type="F_XPATH", get_num=True, split_list={"EUR":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li/div[contains(.,'parking') or contains(.,'Parking')]/div[2]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li/div[contains(.,'balcon')]/div[2]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li/div[contains(.,'Meublé')]/div[2]//text()[not(contains(.,'Non'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li/div[contains(.,'Ascenseur')]/div[2]//text()[not(contains(.,'Non'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li/div[contains(.,'terrasse')]/div[2]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//li/div[contains(.,'Piscine')]/div[2]//text()[not(contains(.,'Non'))]", input_type="F_XPATH", tf_item=True)
        
        energy_label = response.xpath("//li/div[contains(.,'Conso')]/div[2]//text()[not(contains(.,'Vierge'))]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        desc = " ".join(response.xpath("//div[contains(@class,'desc text')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="VOTRE AGENCE C2I BOUILLARGUES", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 30 92 01 17", input_type="VALUE")

        yield item_loader.load_item()