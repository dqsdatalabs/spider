# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'faivre_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.faivre-immo.com/advanced-search/?filter_search_action%5B%5D=location&filter_search_type%5B%5D=appartement&advanced_area=&nombre-de-pieces=&superficie-maximum=&code-postal=&prix-maximum=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.faivre-immo.com/advanced-search/?filter_search_action%5B%5D=location&filter_search_type%5B%5D=maisonvilla&advanced_area=&nombre-de-pieces=&superficie-maximum=&code-postal=&prix-maximum=",
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
        for item in response.xpath("//div[@class='carousel-inner']/following-sibling::a[1]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//li[@class='roundright']/a/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Faivre_Immo_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//span[contains(@style,'#000')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//span[contains(@style,'#000')]/text()", input_type="F_XPATH", split_list={"(":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[contains(@style,'#000')]/text()", input_type="F_XPATH", split_list={"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//h1//text()", input_type="M_XPATH", split_list={"Ref :":1})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'custom_detail')]/p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div/span[contains(.,'Surface')]/following-sibling::text()", input_type="F_XPATH", get_num=True, split_list={"m":0})
        
        if response.xpath("//div/span[contains(.,'Chambres')]/following-sibling::text()[.!='0']"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div/span[contains(.,'Chambres')]/following-sibling::text()[.!='0']", input_type="F_XPATH", get_num=True)
        elif response.xpath("//div/span[contains(.,'Pièces')]/following-sibling::text()[.!='0']"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div/span[contains(.,'Pièces')]/following-sibling::text()[.!='0']", input_type="F_XPATH", get_num=True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div/span[contains(.,'Salle')]/following-sibling::text()[.!='0']", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(@style,'126375')]/span[contains(@class,'price_label price_label_before')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div/span[contains(.,'Etage')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div/span[contains(.,'Parking')]/following-sibling::text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div/span[contains(.,'Balcon')]/following-sibling::text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        
        energy_label = response.xpath("//img/@src[contains(.,'econome_')]").get()
        if energy_label:
            energy_label = energy_label.split("econome_")[1].split(".")[0]
            item_loader.add_value("energy_label", energy_label)
        
        available_date = response.xpath("//div[contains(@class,'custom_detail')]/p//text()[contains(.,'Disponible')]").get()
        if available_date:
            available_date = available_date.split("Disponible")[1].strip()
            if "immédiatement" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                available_date = available_date.split("le")[1].split(".")[0].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='carousel-inner']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Faivre Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="05 61 62 17 09", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@faivre-immo.com", input_type="VALUE")
        
        yield item_loader.load_item()