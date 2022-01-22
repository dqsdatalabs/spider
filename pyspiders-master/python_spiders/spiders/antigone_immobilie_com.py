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
    name = 'antigone_immobilie_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Antigone_Immobilier_PySpider_france'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.antigone-immobilier.com/a-louer/appartements/1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.antigone-immobilier.com/a-louer/maison/1",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//a[contains(.,'voir')]/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen:
            follow_url = response.url.replace("/" + str(page - 1), "/" + str(page))
            yield Request(
                follow_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        external_link = response.url.split("?")[0]
        if external_link and "redac/legals" in external_link.lower():
            return
        else:
            item_loader.add_value("external_link", external_link)
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Antigone_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[contains(@class,'ref')]//text()", input_type="F_XPATH", split_list={"Ref":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//span[contains(.,'Ville')]//following-sibling::span//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//span[contains(.,'Code postal')]//following-sibling::span//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[contains(.,'Ville')]//following-sibling::span//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[contains(@itemprop,'description')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[contains(.,'Surface habitable')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True, split_list={"m":0,",":0})
        if response.xpath("//span[contains(.,'chambre')]//following-sibling::span//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'chambre')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//span[contains(.,'pièce')]//following-sibling::span//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'pièce')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'salle')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(.,'Loyer')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(.,'garantie')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'imageGallery')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//span[contains(.,'Etage')]//following-sibling::span//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[contains(.,'Charges')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//span[contains(.,'Meublé')]//following-sibling::span//text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//span[contains(.,'Ascenseur')]//following-sibling::span//text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//span[contains(.,'Terrasse')]//following-sibling::span//text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//span[contains(.,'parking')]//following-sibling::span//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//span[contains(.,'Balcon')]//following-sibling::span//text()[contains(.,'OUI')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lat')]/text()", input_type="F_XPATH", split_list={"lat :":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lat')]/text()", input_type="F_XPATH", split_list={"lng:":1,"}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="ANTIGONE IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 67 64 52 25", input_type="VALUE")  
        
        title = " ".join(response.xpath("//div[contains(@class,'bienTitle')]//h2//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        yield item_loader.load_item()