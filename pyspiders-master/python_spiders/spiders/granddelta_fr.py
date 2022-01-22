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
    name = 'granddelta_fr'
    execution_type='testing' 
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.granddelta.fr/biens/page/1/?type_de_bien=1100&nombre_de_pieces=&ville=&prix=&type_de_contrat=location&currentpage=1",
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
        for item in response.xpath("//h3[@class='biens-bloc-name']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[@class='next-post']/@href").get()
        if next_page and "javascript" not in next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response) 
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        if "parking" in response.url:
            return 
        if "garage" in response.url:
            return 
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Granddelta_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//p[contains(@class,'localisa')][contains(.,'(')]//text()", input_type="F_XPATH", split_list={"(":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[contains(@class,'price')]//text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[contains(.,'Ref')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div/span[contains(.,'Superficie')]/parent::div/text()", input_type="M_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div/span[contains(.,'Pièces')]/parent::div/text()", input_type="M_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div/span[contains(.,'Disponibilité')]/parent::div/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div/@data-lat", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div/@data-lng", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@data-u='slides']//@src", input_type="M_XPATH")
        
        adres=response.xpath("//title/text()").get()
        if adres:
            item_loader.add_value("address",adres.split(" ")[-2:])
        city=response.xpath("//title/text()").get()
        if city:
            item_loader.add_value("city",city.split(" ")[-2:])

        desc = " ".join(response.xpath("//h3[contains(.,'Description')]//../p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)



        utilities = " ".join(response.xpath("substring-after(//div[@class='col-sm-8 col-xs-12']/p/text()[contains(.,'des charges')],': ')").getall())
        if utilities:
            uti = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", uti)
        
        energy_label = response.xpath("//img/@src[contains(.,'energetique')]").get()
        if energy_label:
            energy_label = energy_label.split("-")[-1].split(".")[0]
            if "nr" not in energy_label:
                item_loader.add_value("energy_label", energy_label.upper())
        
        floor = response.xpath("//h1/text()[contains(.,'ème')]").get()
        if floor:
            floor = floor.split("ème")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//p[@class='fwb']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[contains(@class,'fiche-bien')]//p[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//div[contains(@class,'fiche-bien')]//p/a/text()[contains(.,'@')]", input_type="F_XPATH")
        
        yield item_loader.load_item()