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
    name = 'belharra_immobilier_com' 
    execution_type='testing'
    country='france'
    locale='fr' 
    external_source='Belharra_Immobilier_PySpider_france'
    # custom_settings = {
    #     "PROXY_ON": True
    # }
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.belharra-immobilier.com/catalog/advanced_search_result.php?action=update_search&search_id=&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_30_MIN=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_34_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&C_38_MAX=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_47_type=NUMBER&C_47_search=COMPRIS&C_47_MIN=&C_94_type=FLAG&C_94_search=EGAL&C_94=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.belharra-immobilier.com/catalog/advanced_search_result.php?action=update_search&search_id=1689944298190315&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_30_MIN=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_34_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&C_38_MAX=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_47_type=NUMBER&C_47_search=COMPRIS&C_47_MIN=&C_94_type=FLAG&C_94_search=EGAL&C_94=",
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

        for item in response.xpath("//div[@id='listing_bien']/div//a[contains(.,'Découvrir')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        if response.xpath("//div[@class='title-product']/h1/text()[contains(.,'Studio') or contains(.,'studio')]").get():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url.split('?')[0])
        
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Belharra_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='title-product']/h1/span/text()", input_type="F_XPATH")
        city = response.xpath("//div[@class='title-product']/h1/span/text()").get()
        if city:
            zipcode = city.split(" ")[0]
            city = city.split(zipcode)[1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[@class='title-product']/h1/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[contains(.,'Ref')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(@class,'price')]/text()", input_type="F_XPATH", get_num=True, split_list={"€":0, "Loyer":1})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[contains(@class,'charges')]/text()", input_type="F_XPATH", get_num=True, split_list={"€":0, ":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(@class,'depot')]/text()", input_type="F_XPATH", get_num=True, split_list={"€":0, ":":1})
        # ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[contains(@class,'surface')]/text()", input_type="F_XPATH", get_num=True, split_list={"m²":0, ":":1})
        square_meters=response.xpath("//span[contains(@class,'surface')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].split(".")[0])
        desc = " ".join(response.xpath("//div[@class='description-product']/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc) 
        
        furnished = response.xpath("//h1//text()[contains(.,'MEUBLÉ')]").get()
        if furnished: item_loader.add_value("furnished", True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li//div[contains(.,'chambre')]/text() | //li//div[contains(.,'pièce')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='slider_product']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="BELHARRA IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@belharra-immobilier.com", input_type="VALUE")
    
        item_loader.add_value("landlord_phone"," 05.59.20.90.49")
        yield item_loader.load_item()