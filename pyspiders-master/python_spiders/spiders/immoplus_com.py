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
    name = 'immoplus_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.immoplus.com/locations?t=11",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.immoplus.com/locations?t=12",
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

        for item in response.xpath("//article[@class='listeLigneBien']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        garage = response.xpath("//h1[contains(.,'garage')]/text()").get()
        if garage:
            return

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Immoplus_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH", split_list={" - ":0}, lower_or_upper=1)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@id='ctl00_cphPanMilieu_lblPrix']/text()", input_type="F_XPATH", get_num=True, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[@class='infoReference']/text()", input_type="F_XPATH", split_list={".":1})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[@class='infoDepot']/span/text()", input_type="F_XPATH", get_num=True, split_list={"€":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[@id='ctl00_cphPanMilieu_lblCharges']/text()[.!='0,00']", input_type="F_XPATH", get_num=True, split_list={",":0})
        
        if response.xpath("//span[contains(.,'chambre')]/text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="substring-after(//span[contains(.,'chambre')]/text(),':')", input_type="F_XPATH", get_num=True)
        elif response.xpath("//span[contains(.,'pièces')]/text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="substring-after(//span[contains(.,'pièces')]/text(),':')", input_type="F_XPATH", get_num=True)
            
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//span[contains(.,'Terrass')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//span[contains(.,'Balcon')]/text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//span[contains(.,'Garage')]/text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//span[contains(.,'Ascenseur')]/text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//h1/text()[contains(.,'m²')]", input_type="F_XPATH", get_num=True, split_list={"m²":0, " ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='divGalerieBien']//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lat:":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lng:":1, "}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//span[contains(.,'Etage')]/text()", input_type="F_XPATH", split_list={":":1})
        
        address = response.xpath("//div[@class='bienDetailDesc']/p[1]//text()[2]").get()
        if address:
            zipcode = address.strip().split(" ")[0]
            city = address.split(zipcode)[1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        desc = " ".join(response.xpath("//div[@class='bienDetailDesc']/p[2]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)    
        
        energy_label = response.xpath("//span[@id='ctl00_cphPanMilieu_litDpeEnergetiqueNote']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.upper())
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="IMMO PLUS", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 50 51 62 09", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="", input_type="VALUE")

        yield item_loader.load_item()