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
    name = 'primo_europeenne_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "1",
            },
            {
                "property_type" : "house",
                "type" : "2",
            },
        ]
        for item in start_urls:
            formdata = {
                "nature": "2",
                "type": item["type"],
                "rooms": "",
                "city": "",
                "area_min": "",
                "area_max": "",
                "price_min": "",
                "price_max": "",
            }
            api_url = "http://www.primo-europeenne.com/en/search/"
            yield FormRequest(
                url=api_url,
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                })

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//h2/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        
        prop_type = response.xpath("//h1/text()").get()
        if prop_type and "studio" in prop_type.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta["property_type"])
        
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Primo_Europeenne_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH", split_list={"-":1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={"-":1})

        room = response.xpath("//h2/text()").get()
        if room:
            if "bedroom" in room:
                ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//h2/text()", input_type="F_XPATH", split_list={"bed":0, " ":-1})
            elif "room" in room:
                ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//h2/text()", input_type="F_XPATH", split_list={"room":0, " ":-1})
            
            if "m²" in room:
                ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//h2/text()", input_type="F_XPATH", split_list={"m²":0, " ":-1})
        
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h1/i/text()", get_num=True, input_type="F_XPATH", split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[contains(.,'Reference')]/text()", input_type="F_XPATH", split_list={"Reference":1})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//p[@class='services']//text()[contains(.,'Furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//p[@class='services']//text()[contains(.,'Lift')]", input_type="F_XPATH", tf_item=True)
        
        desc = " ".join(response.xpath("//article/p[not(contains(.,'Reference'))]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        energy_label = response.xpath("//img/@src[contains(.,'dpe1')]").get()
        if energy_label:
            energy_label = energy_label.split("value/")[1].split("/")[0]
            item_loader.add_value("energy_label", energy_label)
        
        if "Bathroom" in desc:
            bathroom_count = desc.split("Bathroom")[0].strip().split(" ")[-1]
            item_loader.add_value("bathroom_count", bathroom_count)

        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p[@class='fees']//text()[contains(.,'Guarantee')]", input_type="M_XPATH", split_list={"Guarantee :":1, "€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//p[@class='fees']//text()[contains(.,'Condominium fees')]", input_type="M_XPATH", split_list={"Condominium fees :":1, "€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='picture']//@href[not(contains(.,'#'))]", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="PRIMO EUROPEENNE", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="33 04 93 88 38 88", input_type="VALUE")
        
        yield item_loader.load_item()
