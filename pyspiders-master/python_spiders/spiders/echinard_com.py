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
    name = 'echinard_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.echinard.com/annonces/liste-appartements-type-1-a-louer-a-beaune-12-0-0.html",
                    "https://www.echinard.com/annonces/liste-appartements-type-2-a-louer-a-beaune-13-0-0.html",
                    "https://www.echinard.com/annonces/liste-appartements-type-3-a-louer-a-beaune-14-0-0.html",
                    "https://www.echinard.com/annonces/liste-appartements-type-4-a-louer-a-beaune-15-0-0.html",
                    "https://www.echinard.com/annonces/liste-appartements-loft-a-louer-a-beaune-27-0-0.html",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.echinard.com/annonces/liste-maisons-a-louer-a-beaune-17-0-0.html",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//li[@class='project-item']//a[@class='more']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[contains(.,'Annonces suivantes')]/@href").get()
        if next_button and not 'javascript:void' in next_button:
            yield Request(
                response.urljoin(next_button),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Echinard_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[contains(.,'Ref')]//text()", input_type="F_XPATH",split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'description')]//text()", input_type="M_XPATH")
        
        title = "".join(response.xpath("//h2[contains(@class,'page-header')]//text()[not(contains(.,'énergétique'))]").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//h1//text()").get()
        if address:
            address = address.replace(", supermarché","")
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)

        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[contains(.,'Surface habitable')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True, split_list={"m":0,".":0})
        if response.xpath("//span[contains(.,'chambre')]//following-sibling::span//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'chambre')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//span[contains(.,'pièce')]//following-sibling::span//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'pièce')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'prix')]//text()", input_type="M_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[contains(@class,'prix_conditions')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1, "€":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div[contains(@class,'prix')]//text()[contains(.,'charge')]", input_type="F_XPATH", get_num=True, split_list={"€":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'slides')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//span[contains(.,'performance énergetique')]//following-sibling::span//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Cabinet Echinard", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="03 80 24 16 27", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="echinard@echinard.com", input_type="VALUE")

        yield item_loader.load_item()