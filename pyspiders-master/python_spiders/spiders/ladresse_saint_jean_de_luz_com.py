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
    name = 'ladresse_saint_jean_de_luz_com'
    execution_type='testing'
    country='france'
    locale='fr'
    # DYNAMIC DATA
    b_url = "https://www.ladresse-saint-jean-de-luz.com"
    source_name = "Ladresse_Saint_Jean_De_Luz_PySpider_france"
    landlord_n = "l'Adresse - SAINT JEAN DE LUZ CEDEX"
    landlord_p = "05.59.51.10.21"
    # ------------
    
    def start_requests(self):
        start_urls = [
            {"url": f"{self.b_url}/catalog/result_carto.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=", "property_type": "apartment"},
            {"url": f"{self.b_url}/catalog/result_carto.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=", "property_type": "house"},
            {"url": f"{self.b_url}/catalog/result_carto.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=17&C_27_tmp=17&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='products-cell']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value=self.source_name, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH", split_list={"à":1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={"à":1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='content-desc']/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//ul/li[2]/span[@class='critere-value']/text()[contains(.,'m²')]", input_type="F_XPATH", get_num=True, split_list={".":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//ul/li/span[@class='critere-value']/text()[contains(.,'Pièces')]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//ul/li/img[contains(@src,'bain')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='product-price'][1]/div/span[@class='alur_loyer_price']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(@class,'depot')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1, ".":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='sliders-product']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lat')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lat')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1, ");":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[contains(@class,'charges')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1, ".":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//ul/li/span[@class='critere-value']//preceding::img/@src[contains(.,'garage')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//ul/li/span[@class='critere-value']/text()[contains(.,'Meublée') or contains(.,'Aménagée') or contains(.,'équipée')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value=self.landlord_n, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value=self.landlord_p, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[@itemprop='name']/text()[contains(.,'Ref.')]", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[img[contains(@src,'etage')]]/span/text()", input_type="F_XPATH", split_list={"/":0})
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[@class='product-dpe'][1]/div/@class", input_type="F_XPATH", split_list={" ":1, "-":1})

        yield item_loader.load_item()