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
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'mb_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.mb-immobilier.com/nos-biens/2-louer?idPage={}&typeBien=2&ajax=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.mb-immobilier.com/nos-biens/2-louer?idPage={}&typeBien=1&ajax=1",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            headers={
                                "accept": "application/json, text/javascript, */*; q=0.01",
                            },
                            meta={'property_type': url.get('property_type'), "base":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        data = json.loads(response.body)
        sel = Selector(text=data["view"], type="html")
        for item in sel.xpath("//a[@class='link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base = response.meta["base"]
            p_url = base.format(page)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "base":base, "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Mb_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='bienLocaV']/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[@class='bienLocaV']/text()", input_type="M_XPATH", split_list={",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='bienLocaV']/text()", input_type="M_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='text']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div/span[contains(.,'habitable')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div/span[contains(.,'Pièce')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div/span[contains(.,'Salle')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div/span[contains(.,'Loyer')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='text']//text()[contains(.,'Disponible')]", input_type="F_XPATH", split_list={"Disponible":1,".":0}, replace_list={"à compter du":"", " le ":"", "debut":""})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='text']//text()[contains(.,'garantie')]", input_type="F_XPATH", get_num=True, split_list={"garantie":1, "euro":0}, replace_list={"de":"","s'élevant à":"", " ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="substring-after(//h4[contains(.,'#')]/text(),'#')", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@id='carousel']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div/span[contains(.,'Charges')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//h1/text()[contains(.,'meublé')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="M&B Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="02 47 66 34 12", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@mb-immobilier.com", input_type="VALUE")

        yield item_loader.load_item()