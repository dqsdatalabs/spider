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
    name = 'chillyimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {"HTTPCACHE_ENABLED": False}

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.chillyimmobilier.fr/annonces-location/appartement/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.chillyimmobilier.fr/annonces-location/maison/",
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

        for item in response.xpath("//p[@class='lien-detail']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        for item in response.xpath("//a[@class='link-bien']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//div[@class='pagelinks-next']/a/@href").get()
        if next_page: yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Chillyimmobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//strong[contains(.,'Réf')]//parent::li/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//strong[contains(.,'Ville')]//parent::li/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//strong[contains(.,'Code')]//parent::li/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//strong[contains(.,'Ville')]//parent::li/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[contains(@class,'desc')]//h2//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='description']//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//strong[contains(.,'Surface habitable')]//parent::li/text()", input_type="F_XPATH", get_num=True, split_list={"m":0,",":0})
        if response.xpath("//strong[contains(.,'chambre')]//parent::li/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//strong[contains(.,'chambre')]//parent::li/text()", input_type="F_XPATH", get_num=True)
        if response.xpath("//strong[contains(.,'pièce')]//parent::li/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//strong[contains(.,'pièce')]//parent::li/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//strong[contains(.,'salle')]//parent::li/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//strong[contains(.,'Loyer')]//parent::li/text()", input_type="M_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//strong[contains(.,'Dépot de garantie')]//parent::li/text()", input_type="M_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//meta[contains(@property,'og:image')]//@content", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,' latitude')]/text()", input_type="F_XPATH", split_list={"latitude =":1,";":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,' latitude')]/text()", input_type="F_XPATH", split_list={"longitude =":1,";":0})
        energy_label = response.xpath("//div[contains(@class,'diagnostic_images')]//@data-src").get()
        if energy_label:
            energy_label = energy_label.split("dpe/")[1].split("/")[0]
            item_loader.add_value("energy_label", energy_label)
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//strong[contains(.,'étages')]//parent::li/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//strong[contains(.,'Charges')]//parent::li/text()", input_type="M_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//strong[contains(.,'parking')]//parent::li/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//strong[contains(.,'Balcon')]//parent::li/text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//strong[contains(.,'Meublé')]//parent::li/text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//strong[contains(.,'Ascenseur')]//parent::li/text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//strong[contains(.,'Terrasse')]//parent::li/text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Chilly Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01 69 09 01 25", input_type="VALUE")
        yield item_loader.load_item()