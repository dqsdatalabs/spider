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
    name = 'proby_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.proby.fr/recherche?f=0&r=-3&etage=0&traml1=0&traml3=0&type=13&b=0&t=0&meuble=0&g=0&loyerx=MIN.&loyern=MAX.&sfrx=MIN.&srfn=MAX.",
                    "https://www.proby.fr/recherche?f=0&r=-3&etage=0&traml1=0&traml3=0&type=14&b=0&t=0&meuble=0&g=0&loyerx=MIN.&loyern=MAX.&sfrx=MIN.&srfn=MAX.",
                    "https://www.proby.fr/recherche?f=0&r=-3&etage=0&traml1=0&traml3=0&type=15&b=0&t=0&meuble=0&g=0&loyerx=MIN.&loyern=MAX.&sfrx=MIN.&srfn=MAX.",
                    "https://www.proby.fr/recherche?f=0&r=-3&etage=0&traml1=0&traml3=0&type=16&b=0&t=0&meuble=0&g=0&loyerx=MIN.&loyern=MAX.&sfrx=MIN.&srfn=MAX.",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.proby.fr/recherche?f=0&r=-3&etage=0&traml1=0&traml3=0&type=19&b=0&t=0&meuble=0&g=0&loyerx=MIN.&loyern=MAX.&sfrx=MIN.&srfn=MAX.",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.proby.fr/recherche?f=0&r=-3&etage=0&traml1=0&traml3=0&type=12&b=0&t=0&meuble=0&g=0&loyerx=MIN.&loyern=MAX.&sfrx=MIN.&srfn=MAX.",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):

        for item in response.xpath("//div[@class='bien list']/div[@class='zoneText']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Proby_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(.,'m²')]/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        
        if response.xpath("//li[contains(.,'Studio') or contains(.,'studio')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="1", input_type="VALUE")
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Pièce') or contains(.,'pièce')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'garantie')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1,"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[contains(@class,'ref')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='gal']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='tarif']/p[contains(@class,'pri')]/text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.lower().split("appartement")[0].strip().capitalize())
        
        floor = response.xpath("//li[contains(.,'Etage') or contains(.,'Rdc')]/text()").get()
        if floor:
            if "rdc" in floor.lower():
                item_loader.add_value("floor", floor)
            else:
                floor = floor.split(" ")[1]
                item_loader.add_value("floor", floor)
        
        desc = " ".join(response.xpath("//p[contains(@class,'desc')]/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        import dateparser
        if "Disponible le" in desc:
            available_date = desc.split("Disponible le")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
                
        energy_label = response.xpath("//img/@src[contains(.,'dpe=')]").get()
        if energy_label:
            energy_label = energy_label.split("dpe=")[1].split("&")[0]
            item_loader.add_value("energy_label", energy_label)

        balcony = response.xpath("//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="PROBY", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 67 10 13 10", input_type="VALUE")

        if response.xpath("//li[contains(.,'Garage') or contains(.,'garage')]").get(): item_loader.add_value("parking", True)
        
        yield item_loader.load_item()