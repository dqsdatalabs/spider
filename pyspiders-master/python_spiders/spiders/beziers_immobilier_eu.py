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
    name = 'beziers_immobilier_eu'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Beziers_Immobilier_PySpider_france'

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.beziers-immobilier.eu/catalog/advanced_search_result.php?action=update_search&search_id=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_MAX=&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_34_MAX=&C_33_MAX=&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&keywords=",
                "property_type": "apartment"
            },

        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'cell-product')]/a//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get('property_type')})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        from python_spiders.helper import ItemClear
        
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1//text()", input_type="F_XPATH", split_list={"-":0},replace_list={"\u00c9":"","\u00e8":"","\u00b2":""})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1//span[@class='ville-title']//text()", input_type="F_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1//span[@class='ville-title']//text()", input_type="F_XPATH", split_list={" ":0},replace_list={"\u00c9":""})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title//text()", input_type="F_XPATH",replace_list={"\u00c9":"","\u00e8":"","\u00b2":""})
        external_id = response.url
        if external_id:
            external_id = external_id.split('fiches/')[-1].split('/')[0].strip()
            item_loader.add_value("external_id", external_id)
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li//div[contains(.,'Surface')]//following-sibling::div//text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ",":0},replace_list={".":""})

        studio_check = response.xpath("//h1[@class='product-title']/text()").get()
        if studio_check and "studio" in studio_check.lower():
            item_loader.add_value("room_count", 1)
        else:
            if response.xpath("//li//div[contains(.,'pièce')]//following-sibling::div//text()"):
                ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li//div[contains(.,'pièce')]//following-sibling::div//text()", input_type="F_XPATH", get_num=True)
            elif response.xpath("//li//div[contains(.,'chambre')]//following-sibling::div//text()"):
                ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li//div[contains(.,'chambre')]//following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li//div[contains(.,'Salle')]//following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='prix loyer']//span[@class='alur_loyer_price'][1]//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[@class='alur_location_depot']//text()", input_type="F_XPATH", get_num=True, split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li//div[contains(.,'Nombre étages')]//following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//span[@class='alur_location_meuble']//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='slider_product_large']//div//a//img//@src", input_type="M_XPATH")
        
        desc = " ".join(response.xpath("//div[@class='products-description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.replace("\u00e9","").replace("\u00a0","").replace("\u20ac","").replace("\u00e8","").replace("\u00e0","").replace("\u00f4","").replace("\u00c9","").replace("\u00b2",""))
        
        energy_label = response.xpath("//li//div[contains(.,'Consommation énergie primaire')]//following-sibling::div//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        


        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Agence France Sud Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="33467491260", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="agence@fsibeziers.fr", input_type="VALUE")

        yield item_loader.load_item()