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
    name = 'marylinegiraudgestion_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.marylinegiraudgestion.fr/annonces-location/appartement/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.marylinegiraudgestion.fr/annonces-location/maison/",
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

        for item in response.xpath("//ul/li//p[@class='lien-detail']/a/@href").getall():
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
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Marylinegiraudgestion_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//li[contains(.,'R??f')]/strong//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//li[contains(.,'Ville') or contains(.,'D??partement')]/strong//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//li[contains(.,'Code postal')]/strong//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//li[contains(.,'Ville')]/strong//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[contains(@class,'description')]//h2[contains(@class,'h2puce')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[contains(@itemprop,'description')]/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(.,'Surface habitable')]/strong//text()", input_type="F_XPATH", get_num=True, split_list={"m":0,",":0})
        # if response.xpath("//li[contains(.,'chambres')]/strong//text()").get():
        #     ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'chambres')]/strong//text()", input_type="F_XPATH", get_num=True)
        if response.xpath("//li[contains(.,'pi??ces')]/strong//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'pi??ces')]/strong//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'salle')]/strong//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li[contains(.,'Loyer')]/strong/text()", input_type="F_XPATH", get_num=True, split_list={"???":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[contains(.,'Etage')]/strong//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'Disponibilit??')]/strong/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'D??pot de garantie')]/strong/text()", input_type="F_XPATH", get_num=True, split_list={"???":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges')]/strong/text()", input_type="F_XPATH", get_num=True, split_list={"???":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'garage')]/strong//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon')]/strong//text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Meubl??')]/strong//text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Ascenseur')]/strong//text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrasse')]/strong//text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@id,'photoslider')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'latitude')]/text()", input_type="M_XPATH", split_list={" latitude =":1,";":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'latitude')]/text()", input_type="M_XPATH", split_list={" longitude =":1,";":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Maryline Giraud Gestion", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="06 62 36 52 88", input_type="VALUE")

        yield item_loader.load_item()