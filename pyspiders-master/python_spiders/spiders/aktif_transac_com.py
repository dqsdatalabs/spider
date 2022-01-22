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
    name = 'aktif_transac_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_url = "https://aktif-transac.com/biens"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        data = response.xpath("//script[contains(.,'window.__NUXT')]/text()").get()
        if data:
            apartment_filter = 'TYPE_OFFRE:{_cdata:"11"}'
            if len(data.split(apartment_filter)) > 0:
                for i in range(1, len(data.split(apartment_filter))):
                    p_id = data.split(apartment_filter)[i].split('NO_DOSSIER:{_cdata:"')[1].split('"')[0].strip()
                    yield Request(f"https://aktif-transac.com/biens/{p_id}", callback=self.populate_item, meta={"property_type":"apartment"})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)

        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Aktif_Transac_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//aside/h4[contains(.,'Lieu')]/following-sibling::h3/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//aside/h4[contains(.,'Lieu')]/following-sibling::h3/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//aside/h4[contains(.,'Prix')]/following-sibling::h3/text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//aside/h4[contains(.,'Superficie')]/following-sibling::h3/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//aside/h4[contains(.,'pièce')]/following-sibling::h3/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//aside/h4[contains(.,'Balcon')]/following-sibling::h3/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        
        desc = " ".join(response.xpath("//meta[@name='description']/@content").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//img/@src[contains(.,'jpg')]", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Aktif transac", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="33467171806", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact.aktif@gmail.com", input_type="VALUE")

        yield item_loader.load_item()