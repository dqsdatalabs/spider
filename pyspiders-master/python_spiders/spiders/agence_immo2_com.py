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
    name = 'agence_immo2_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [

            {
                "url" : [
                    "https://agence-immo2.com/advanced-search/?keyword=&status=location&type=appartement&bedrooms=&min-area=&max-price=&bathrooms=&max-area=&min-price=",
                ],
                "property_type" : "apartment",
                "url" : [
                    "https://agence-immo2.com/advanced-search/?keyword=&status=location&type=maison&bedrooms=&min-area=&max-price=&bathrooms=&max-area=&min-price=",
                ],
                "property_type" : "house",
                "url" : [
                    "https://agence-immo2.com/advanced-search/?keyword=&status=location&type=studio&bedrooms=&min-area=&max-price=&bathrooms=&max-area=&min-price=",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'property-listing')]/div/div[contains(@id,'ID')]//div[contains(@class,'body-right')]//a[contains(.,'Détails')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[@rel='Next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agence_Immo2_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//span/span[contains(.,'Lieu')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span/span[contains(.,'Lieu')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//li/strong[contains(.,'Code')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span/span[contains(.,'Chambre')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span/span[contains(.,'Surface')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li//strong[contains(.,'Loyer')]/following-sibling::label/text()", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={".":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span/span[contains(.,'Référence')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li//strong[contains(.,'Charges')]/following-sibling::label/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li//strong[contains(.,'Salle')]/following-sibling::label/text()", input_type="F_XPATH", get_num=True)
        
        deposit = response.xpath("//strong[contains(.,'Dépôt de Garantie')]//following-sibling::label//text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip()
            item_loader.add_value("deposit", deposit)

        energy_label = response.xpath("//h5[contains(.,'DPE')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[1].split("(")[0])
        
        desc = " ".join(response.xpath("//div[@id='description']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        elevator = response.xpath("//li[contains(.,'Ascenseur')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        parking = response.xpath("//li[contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]//text()", input_type="F_XPATH", split_list={'lat":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]//text()", input_type="F_XPATH", split_list={'lng":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'lightbox-slide')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//dd/i[contains(@class,'user')]/parent::dd/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//span/i[contains(@class,'phone')]/parent::span/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="immo2-bouzidi@orange.fr", input_type="VALUE")
        
        yield item_loader.load_item()