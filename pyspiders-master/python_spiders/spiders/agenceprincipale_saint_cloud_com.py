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
    name = 'agenceprincipale_saint_cloud_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Agenceprincipale_Saint_Cloud_PySpider_france'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agenceprincipale-saint-cloud.com/catalog/advanced_search_result.php?action=update_search&search_id=1689938433646718&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_30_search=COMPRIS&C_30_type=NUMBER&C_33_search=COMPRIS&C_33_type=NUMBER&C_33=&C_33_MAX=&C_34_search=COMPRIS&C_34_type=NUMBER&C_28_tmp=Location&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_34_MIN=&C_34_MAX=&C_30_MIN=&C_30_MAX=&C_33_MIN=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agenceprincipale-saint-cloud.com/catalog/advanced_search_result.php?action=update_search&search_id=1689938433646718&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_30_search=COMPRIS&C_30_type=NUMBER&C_33_search=COMPRIS&C_33_type=NUMBER&C_33=&C_33_MAX=&C_34_search=COMPRIS&C_34_type=NUMBER&C_28_tmp=Location&C_27_tmp=2&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_34_MIN=&C_34_MAX=&C_30_MIN=&C_30_MAX=&C_33_MIN=",
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

        for item in response.xpath("//div[@id='listing_bien']/div[contains(@id,'product')]/div[@class='listing-cell']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agenceprincipale_Saint_Cloud_PySpider_france", input_type="VALUE")
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//li//div[contains(.,'Ville')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//li//div[contains(.,'Ville')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//li//div[contains(.,'Code')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li//div[contains(.,'Loyer charge')]/following-sibling::div//text()", input_type="F_XPATH", split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li//div[contains(.,'sur charges')]/following-sibling::div//text()", input_type="F_XPATH", split_list={" ":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li//div[contains(.,'Garantie')]/following-sibling::div//text()", input_type="F_XPATH", split_list={" ":0, ".":0})

        square_meters = response.xpath("//li//div[contains(.,'Surface')]/following-sibling::div//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.split(" ")[0])))
        
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li//div[contains(.,'Meublé')]/following-sibling::div//text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li//div[contains(.,'Salle')]/following-sibling::div//text()", input_type="F_XPATH")
        
        if response.xpath("//li//div[contains(.,'Chambres')]/following-sibling::div//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li//div[contains(.,'Chambres')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li//div[contains(.,'pièce')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li//div[.='Etage']/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li//div[contains(.,'terrasse')]/following-sibling::div//text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li//div[contains(.,'parking')]/following-sibling::div//text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li//div[contains(.,'balcon')]/following-sibling::div//text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//li//div[contains(.,'Conso')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li//div[contains(.,'Ascensur')]/following-sibling::div//text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        
        desc = " ".join(response.xpath("//div[@class='product-desc']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[contains(.,'Ref')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='slide']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@class='title-bloc']//p/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='tel-manufacturer']//p/text()", input_type="F_XPATH")
        
        yield item_loader.load_item()