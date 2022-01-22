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
    name = 'cabinetmolieres_com'
    execution_type='testing'
    country='france'
    locale='fr'
    url = "https://www.agenceimmo-molieres.com/cabinet_molieres/produits_pages.php"
    formdata = {
        'page': '0',
        'id_categorie': '2',
        'id_sous_categorie': '0',
        'id_sous_sous_categorie': '0',
        'id_marques': '0',
        'item_per_page': '12',
        'id_types_biens': '',
        'surf_hab_min': '0',
        'surf_hab_max': '0',
        'nb_pieces_min': '0',
        'nb_pieces_max': '0',
        'prix_min': '0',
        'prix_max': '0',
        'cp_offre': '0',
        'quartier': '0',
        'no_mandat': '0',
        'categorie': '',
    }
    headers = {
        'Connection': 'keep-alive',
        'Accept': 'text/html, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://www.agenceimmo-molieres.com',
        'Referer': 'https://www.agenceimmo-molieres.com/location/',
        'Accept-Language': 'tr,en;q=0.9',
    }

    def start_requests(self):
        self.formdata["id_types_biens"] = "11"
        yield FormRequest(self.url, formdata=self.formdata, headers=self.headers, dont_filter=True, callback=self.parse, meta={'property_type': "apartment", "type": "11"})
        self.formdata["id_types_biens"] = "12"
        yield FormRequest(self.url, formdata=self.formdata, headers=self.headers, dont_filter=True, callback=self.parse, meta={'property_type': "house", "type": "12"})

    def parse(self, response):

        page = response.meta.get("page", 1)
        seen = False

        for item in response.xpath("//a[@class='article']/@href").getall():
            seen = True
            yield Request("https://www.agenceimmo-molieres.com/" + item, callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 1 or seen:
            self.formdata["id_types_biens"] = response.meta["type"]
            self.formdata["page"] = str(page)
            yield FormRequest(self.url,
                         formdata=self.formdata, 
                         headers=self.headers, 
                         dont_filter=True, 
                         callback=self.parse, 
                         meta={'property_type': response.meta["property_type"], "type": response.meta["type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        prop_type = response.xpath("//div[contains(@class,'row-caract')]//div[contains(.,'Catégorie')]/following-sibling::div/text()[not(contains(.,'Non'))]").get()
        if "studio" in prop_type.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Cabinetmolieres_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[contains(@class,'row-caract')]//div[contains(.,'Ville')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[contains(@class,'row-caract')]//div[contains(.,'Ville')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[contains(@class,'row-caract')]//div[contains(.,'Code')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[contains(@class,'row-caract')]//div[contains(.,'habita')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={"m":0})
        
        if response.xpath("//div[contains(@class,'row-caract')]//div[contains(.,'chambre')]/following-sibling::div/text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@class,'row-caract')]//div[contains(.,'chambre')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//div[contains(@class,'row-caract')]//div[contains(.,'pièce')]/following-sibling::div/text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@class,'row-caract')]//div[contains(.,'pièce')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[contains(@class,'row-caract')]//div[contains(.,'salle')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div[contains(@class,'row-caract')]//div[contains(.,'étages')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'row-caract')]//div[contains(.,'Prix')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[contains(@class,'row-caract')]//div[contains(.,'Ascenseur')]/following-sibling::div/text()[not(contains(.,'Non'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[contains(@class,'row-caract')]//div[contains(.,'Balcon')]/following-sibling::div/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="substring-after(//div[@class='ref_detail_bien']/text(),':')", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//a/@href[contains(.,'map')]", input_type="F_XPATH", split_list={"/@":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//a/@href[contains(.,'map')]", input_type="F_XPATH", split_list={"/@":1,",":1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='slides']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Cabinet Molieres", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="05 61 21 63 45", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="toulouse@cabinetmolieres.com", input_type="VALUE")

        desc = " ".join(response.xpath("//div[@class='texte_detail_bien']/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        yield item_loader.load_item()