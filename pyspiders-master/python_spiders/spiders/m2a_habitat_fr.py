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
    name = 'm2a_habitat_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://m2a-habitat.fr/devenir-locataire/nos-logements/?typeTransaction=r&typeBien=appartement&loyerMax=&surfaceMini=",
                    "https://m2a-habitat.fr/devenir-locataire/nos-logements/?typeTransaction=r&typeBien=duplex&loyerMax=&surfaceMini=",
                    "https://m2a-habitat.fr/devenir-locataire/nos-logements/?typeTransaction=r&typeBien=t3&loyerMax=&surfaceMini=",
                    "https://m2a-habitat.fr/devenir-locataire/nos-logements/?typeTransaction=r&typeBien=t4&loyerMax=&surfaceMini=",
                    "https://m2a-habitat.fr/devenir-locataire/nos-logements/?typeTransaction=r&typeBien=t5&loyerMax=&surfaceMini=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://m2a-habitat.fr/devenir-locataire/nos-logements/?typeTransaction=r&typeBien=studio&loyerMax=&surfaceMini=",
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

        for item in response.xpath("//a[contains(@class,'logement-card')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="M2a_Habitat_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//p[@class='single-title']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='label'][contains(.,'Ville')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='label'][contains(.,'Ville')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@class='label'][contains(.,'Surface')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={"m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='prix-1']/text()[contains(.,'Loyer')]", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div[@class='prix-2']/text()[contains(.,'Charges')]", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[@class='identifiant']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='label'][contains(.,'pièce')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='item']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LngLat([')]/text()", input_type="F_XPATH", split_list={'LngLat([':1, ',':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LngLat([')]/text()", input_type="F_XPATH", split_list={'LngLat([':1, ',':1, "]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="M2A HABITAT", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="03 89 36 28 40", input_type="VALUE")
        
        desc = " ".join(response.xpath("//h2[contains(.,'Description')]//../p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        yield item_loader.load_item()