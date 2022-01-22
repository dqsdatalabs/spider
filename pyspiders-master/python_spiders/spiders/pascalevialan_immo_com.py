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
    name = 'pascalevialan_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = { 
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.pascalevialan-immo.com/a-louer/appartements/{}",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.pascalevialan-immo.com/a-louer/maison-villa/{}",
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

        for item in response.xpath("//span[contains(.,'Voir le bien')]/../@href").getall():
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

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Pascalevialan_Immo_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="normalize-space(//div[@class='bienTitle']/h2/text())", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//li[@class='ref']/text()", input_type="F_XPATH", split_list={"Ref":1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//p[@class='data']/span[contains(.,'Code')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p[@class='data']/span[contains(.,'Ville')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p[@class='data']/span[contains(.,'Ville')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//p[@class='data']/span[contains(.,'habitable')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ",":0})
        
        if response.xpath("//p[@class='data']/span[contains(.,'chambre')]/following-sibling::span/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p[@class='data']/span[contains(.,'chambre')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//p[@class='data']/span[contains(.,'pièces')]/following-sibling::span/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p[@class='data']/span[contains(.,'pièces')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p[@class='data']/span[contains(.,'salle')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//p[@class='data']/span[contains(.,'Etage')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//p[@class='data']/span[contains(.,'Meublé')]/following-sibling::span/text()[not(contains(.,'Non')) and not(contains(.,'NON'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//p[@class='data']/span[contains(.,'Ascenseur')]/following-sibling::span/text()[not(contains(.,'Non')) and not(contains(.,'NON'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[@class='data']/span[contains(.,'Loyer')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p[@class='data']/span[contains(.,'garantie')]/following-sibling::span/text()[not(contains(.,'Non'))]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//p[@class='data']/span[contains(.,'Charges')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'imageGallery')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lat :":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lng:":1, "}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//span[contains(.,'Terrasse')]/following-sibling::span/text()[contains(.,'OUI') or contains(.,'Oui') or contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//span[contains(.,'Balcon')]/following-sibling::span/text()[contains(.,'OUI') or contains(.,'Oui') or contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Pascalevialan Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 68 42 83 00", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="pascale.vialan@wanadoo.fr", input_type="VALUE")

        desc = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        yield item_loader.load_item()