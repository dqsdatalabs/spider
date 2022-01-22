# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'c_immo_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.c-immo.fr/index_catalogue.php?marche=1&transaction=2&type=1&prix_f=&prix_t=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.c-immo.fr/index_catalogue.php?marche=1&transaction=2&type=8&prix_f=&prix_t=",
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

        for item in response.xpath("//a[contains(.,'Lire la suite')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[contains(@class,'a_pages_next')]/@href").get()
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
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="C_Immo_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[contains(.,'Référence')]//following-sibling::span//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//span[contains(@class,'line-1')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1//span[contains(@class,'line-2')]//text()", input_type="F_XPATH", split_list={"/":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1//span[contains(@class,'line-2')]//text()", input_type="F_XPATH", split_list={"/":0,"-":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1//span[contains(@class,'line-2')]//text()", input_type="F_XPATH",split_list={"/":0,"-":1})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[contains(@class,'description')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[contains(.,'Surface')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True, split_list={"m":0,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(@class,'price')]//text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        if response.xpath("//span[contains(.,'Chambre')]//following-sibling::span//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'Chambre')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//span[contains(.,'Pièce')]//following-sibling::span//text()").getall():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'Pièce')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'Salle')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//span[contains(.,'Salle')]//following-sibling::span//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div[contains(@class,'description-price')]//p[contains(.,'de charges locatives')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//span[contains(.,'Ascenseur')]//following-sibling::span//text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="C'IMMO", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 34 00 90 90", input_type="VALUE")

        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//p[contains(@class,'description')]//text()[contains(.,'disponible au')]").getall())
        if available_date:
            available_date = available_date.split("disponible au")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = "".join(response.xpath("//p[contains(@class,'description')]//text()[contains(.,'Dépôt de garantie')]").getall())
        if deposit:
            deposit = deposit.split("Dépôt de garantie")[1].split(":")[1].split("€")[0].strip().split(",")[0].split(".")[0]
            item_loader.add_value("deposit", deposit)
        images = response.xpath("//div[contains(@class,'description-photos')]//@data-src").getall()
        for image in images:
            item_loader.add_value("images", image)

        yield item_loader.load_item()