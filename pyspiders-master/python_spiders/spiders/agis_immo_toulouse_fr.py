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
    name = 'agis_immo_toulouse_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.agis-immo-toulouse.fr/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=1&px_loyermax=Max&px_loyermin=Min&saisie=O%c3%b9+d%c3%a9sirez-vous+habiter+%3f&surfacemax=Max&surfacemin=Min&tri=d_dt_crea&annlistepg={}",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.agis-immo-toulouse.fr/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=2&px_loyermax=Max&px_loyermin=Min&saisie=O%c3%b9+d%c3%a9sirez-vous+habiter+%3f&surfacemax=Max&surfacemin=Min&tri=d_dt_crea&annlistepg={}",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@id='recherche-resultats-listing']/div/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))        
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agis_Immo_Toulouse_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1//text()[2]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1//text()[2]", input_type="F_XPATH", split_list={"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1//text()[2]", input_type="F_XPATH", split_list={"(":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[contains(@class,'reference')]/span/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@itemprop='price']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//strong[contains(.,'garantie')]/text()[not(contains(.,'N/A'))]", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li/div[contains(.,'Surface')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={"m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li/div[contains(.,'Etage')]/following-sibling::div/text()", input_type="F_XPATH")
        
        if response.xpath("//li/div[contains(.,'Chambre')]/following-sibling::div/text()[not(contains(.,'0'))]").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/div[contains(.,'Chambre')]/following-sibling::div/text()[not(contains(.,'0'))]", input_type="F_XPATH", get_num=True)
        elif response.xpath("//li/div[contains(.,'Pièce')]/following-sibling::div/text()[not(contains(.,'0'))]").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/div[contains(.,'Pièce')]/following-sibling::div/text()[not(contains(.,'0'))]", input_type="F_XPATH", get_num=True)  
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li/div[contains(.,'Salle')]/following-sibling::div/text()[not(contains(.,'0'))]", input_type="F_XPATH", get_num=True)  
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li/div[contains(.,'Balcon')]/following-sibling::div/text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li/div[contains(.,'Parking')]/following-sibling::div/text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li/div[contains(.,'Parking')]/following-sibling::div/text()[not(contains(.,'0')) or contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li/div[contains(.,'Meublé')]/following-sibling::div/text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li/div[contains(.,'Ascenseur')]/following-sibling::div/text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='slider']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'MAP_CENTER_LATITUDE')]/text()", input_type="F_XPATH", split_list={'LATITUDE_CARTO: "':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'MAP_CENTER_LATITUDE')]/text()", input_type="F_XPATH", split_list={'LONGITUDE_CARTO: "':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="AGIS IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="3561536207", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="agis.immobilier@orange.fr", input_type="VALUE")
        
        desc = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        energy_label = response.xpath("//div[contains(@class,'dpe-bloc-lettre')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
            
        yield item_loader.load_item()