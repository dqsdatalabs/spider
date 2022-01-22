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
from python_spiders.helper import ItemClear


class MySpider(Spider):
    name = 'leconte_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.leconte-immobilier.com/index.php?contr=biens_liste&tri_lots=date&type_transaction=1&type_lot%5B%5D=appartement&localisation=&hidden-localisation=&nb_piece=&surface=&budget_min=&budget_max=&page=0&vendus=0&submit_search_0=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.leconte-immobilier.com/index.php?contr=biens_liste&tri_lots=date&type_transaction=1&type_lot%5B%5D=maison&localisation=&hidden-localisation=&nb_piece=&surface=&budget_min=&budget_max=&page=0&vendus=0&submit_search_0=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='col-md-7']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Leconte_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h1/text()", input_type="F_XPATH", split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h3/text()[not(contains(.,'Loi'))]", input_type="F_XPATH", split_list={"Simplexml":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h3/text()[not(contains(.,'Loi'))]", input_type="F_XPATH", split_list={" ":0})
      
        room_count = response.xpath("//img[contains(@src,'plan')]/following-sibling::text()").get()
        if "studio" in room_count:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="1", input_type="VALUE")
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//img[contains(@src,'lit')]/following-sibling::text()", input_type="F_XPATH", split_list={" ":0})
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//img[contains(@src,'douche') or contains(@src,'baignoire')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//img[contains(@src,'immeuble')]/following-sibling::text()", input_type="F_XPATH", split_list={" ":0})
            
        square_meters = response.xpath("//img[contains(@src,'metre')]/following-sibling::text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        zipcode = response.xpath("//h1/text()").get()
        if zipcode: item_loader.add_value("zipcode", zipcode.split('-')[-1].strip().split(' ')[0].strip())
        
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'L.map')]/text()", input_type="F_XPATH", split_list={"setView([":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'L.map')]/text()", input_type="F_XPATH", split_list={"setView([":1, ",":1, "]":0})

        desc = " ".join(response.xpath("//div[contains(@class,'col-md-12 col-xs-12')][2]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//img[contains(@src,'balcon')]/@src", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//h1/text()[contains(.,'meublé') or contains(.,'meuble')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//img[contains(@src,'ascenseur')]/@src", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='carousel-inner']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='panel-body']//text()[contains(.,'de garantie')]", get_num=True, input_type="F_XPATH", split_list={",":0, ":":1}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div[@class='panel-body']//text()[contains(.,'Charge')]", get_num=True, input_type="F_XPATH", split_list={",":0, ":":1}, replace_list={" ":""})      
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="LECONTE IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01 43 22 81 23", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="adresse@fournisseur.com", input_type="VALUE")
        
        yield item_loader.load_item()