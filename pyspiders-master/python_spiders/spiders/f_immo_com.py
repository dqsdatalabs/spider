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
    name = 'f_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.f-immo.com/moteur,prevalidation.htm?idqfix=1&idtt=1&idtypebien=1&saisie=O%C3%B9+d%C3%A9sirez-vous+habiter+%3F&idq=&div=&idpays=&cp=&ci=&px_loyermin=Min&px_loyermax=Max&surfacemin=Min&surfacemax=Max&tri=d_dt_crea",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='span9']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="F_Immo_PySpider_france", input_type="VALUE")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

            title = title.split("-")[1].strip().split(" ")
            item_loader.add_value("address", title[0])
            item_loader.add_value("city", title[0])
            item_loader.add_value("zipcode", title[-1].replace("(","").replace(")",""))
            item_loader.add_value("floor", title[1].replace("\u00e8me",""))
            
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@itemprop='price']/text()", input_type="F_XPATH", get_num=True, replace_list={"\xa0":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[contains(@class,'span6 ')]/div[contains(.,'m²')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        
        if response.xpath("//div[contains(@class,'span6 ')]/div[contains(.,'chambre')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@class,'span6 ')]/div[contains(.,'chambre')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@class,'span6 ')]/div[contains(.,'pièce')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
            
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[contains(.,'Référe')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//strong[contains(.,'de garantie')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1, "€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1, "€":0})
        
        desc = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='slider']//@href", input_type="M_XPATH")
        
        if "salle" in desc:
            bathroom_count = desc.split("salle")[0].strip().split(" ")[-1]
            if "une" in bathroom_count.lower():
                item_loader.add_value("bathroom_count", "1")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LONGITUDE')]/text()", input_type="F_XPATH", split_list={'AGLATITUDE: "':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LONGITUDE')]/text()", input_type="F_XPATH", split_list={'AGLONGITUDE: "':1, '"':0})
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="LA FRANCAISE IMMOBILIERE", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="33 0 1 42 850 105", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@f-immo.com", input_type="VALUE")

        yield item_loader.load_item()