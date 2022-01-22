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
    name = 'grandsiecleimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.grandsiecleimmo.com/catalog/advanced_search_result.php?action=update_search&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_LOC_tmp=0&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_30_VENTE_tmp=0&C_34_MAX=&C_33_MIN=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&search_id=1692661664969979&&search_id=1692661664969979&page=1",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='item-listing']//div[@class='infos-product']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//li[contains(@class,'next-link') and contains(@class,'active')]/a/@href").get()
        if next_page: yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Grandsiecleimmo_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/..//div[@class='product-localisation']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//li/div/div[contains(.,'Code')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//li/div/div[contains(.,'Ville')]/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='desc-text']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="substring-after(//span[@class='alur_location_surface']/text(),':')", input_type="F_XPATH", get_num=True, split_list={"m":0, ".":0})
        
        if response.xpath("//li/div/div[contains(.,'Chambre')]/following-sibling::div//text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/div/div[contains(.,'Chambre')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/div/div[contains(.,'pièce')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True)
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li/div/div[contains(.,'Salle')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='alur_loyer_price']/text()", input_type="F_XPATH", get_num=True, split_list={"€":0, " ":-1, ".":0}, replace_list={"\u00a0":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="substring-after(//span[@class='alur_location_depot']/text(),':')", input_type="F_XPATH", get_num=True, split_list={"€":0, ".":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="substring-after(//li/span[contains(.,'Ref')]/text(),':')", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='slider_product']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li/div/div[.='Etage']/following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li/div/div[contains(.,'sur charges')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True, split_list={" ":0,".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li/div/div[contains(.,'parking')]/following-sibling::div//text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li/div/div[contains(.,'balcon')]/following-sibling::div//text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li/div/div[contains(.,'Meublé')]/following-sibling::div//text()[not(contains(.,'Non'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li/div/div[contains(.,'Ascenseur')]/following-sibling::div//text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        item_loader.add_xpath("energy_label", "//li/div/div[contains(.,'Conso Energ')]/following-sibling::div//text()")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="GRAND SIECLE IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01 39 50 93 31", input_type="VALUE")

        yield item_loader.load_item()