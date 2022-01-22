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
    name = 'parlezmoidimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.parlezmoidimmo.fr/annonces-location/appartement/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.parlezmoidimmo.fr/annonces-location/maison/",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='liste-offres']/li//p[@class='lien-detail']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//div[@class='pagelinks-next']/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Parlezmoidimmo_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[@class='description']/h2/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//li/strong[contains(.,'Ville')]/following-sibling::text() | //li/strong[contains(.,'Département')]/following-sibling::text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//li/strong[contains(.,'Ville')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//li/strong[contains(.,'Code')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//li/strong[contains(.,'Réf')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li/strong[contains(.,'Loyer')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li/strong[contains(.,'Charges')]/following-sibling::text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li/strong[contains(.,'garantie')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        
        if response.xpath("//li/strong[contains(.,'habitable')]/following-sibling::text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li/strong[contains(.,'habitable')]/following-sibling::text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ",":0})
        if response.xpath("//li/strong[contains(.,'Surface')]/following-sibling::text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li/strong[contains(.,'Surface')]/following-sibling::text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ",":0})
        
        if response.xpath("//li/strong[contains(.,'chambres')]/following-sibling::text()[.!='0']").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/strong[contains(.,'chambres')]/following-sibling::text()[.!='0']", input_type="F_XPATH", get_num=True)
        elif response.xpath("//li/strong[contains(.,'pièce')]/following-sibling::text()[.!='0']").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/strong[contains(.,'pièce')]/following-sibling::text()[.!='0']", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li/strong[contains(.,'salle')]/following-sibling::text()[.!='0']", input_type="F_XPATH", get_num=True)
            
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li/strong[contains(.,'Meublé')]/following-sibling::text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li/strong[contains(.,'Terrasse')]/following-sibling::text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li/strong[contains(.,'Balcon')]/following-sibling::text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li/strong[contains(.,'Ascenseur')]/following-sibling::text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='photoslider']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li/strong[contains(.,'Disponibilité')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li/strong[contains(.,'Etage')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li/strong[contains(.,'parking') or contains(.,'garage ')]/following-sibling::text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,' longitude')]/text()", input_type="F_XPATH", split_list={" latitude =":1, ";":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,' longitude')]/text()", input_type="F_XPATH", split_list={" longitude =":1, ";":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Parlez-moi d'immo", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 77 76 30 06", input_type="VALUE")
        
        desc = " ".join(response.xpath("//div[@class='description']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "studio" in desc.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        energy_label = response.xpath("//img/@data-src[contains(.,'dpe')][not(contains(.,'vierge'))]").get()
        if energy_label:
            energy_label = energy_label.split("dpe/")[1].split("/")[0]
            item_loader.add_value("energy_label", energy_label)
        
        yield item_loader.load_item()