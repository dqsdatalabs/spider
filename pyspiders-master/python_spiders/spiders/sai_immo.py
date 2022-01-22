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
    name = 'sai_immo'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://sai.immo/resultat-de-la-recherche/?status=location&search_text=&location=-1&info%5Bparking%5D=&info%5Bbedrooms%5D=&types=appartement&min_price=0&max_price=1000000&min_area=0&max_area=2000",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://sai.immo/resultat-de-la-recherche/?status=location&search_text=&location=-1&info%5Bparking%5D=&info%5Bbedrooms%5D=&types=maison&min_price=0&max_price=1000000&min_area=0&max_area=2000",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='property-list-style']/meta/@content").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[@aria-label='Suivant']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Sai_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//span[@class='property-locations']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[@class='property-locations']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//header//div[@class='property-price']/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='entry-content']//p//text()[contains(.,'garantie')]", input_type="F_XPATH", get_num=True, split_list={":":1, " ":0, ",":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='owl-carousel']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div/@data-latitude", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div/@data-longitude", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='entry-content']//p//text()[contains(.,'Reference')]", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking :')]/text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        
        desc = " ".join(response.xpath("//div[@class='entry-content']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "studio" in desc.lower():
            item_loader.add_value("property_type", "studio")
            item_loader.add_value("room_count", "1")
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Chambres :')]/text()", input_type="F_XPATH", get_num=True)
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        if response.xpath("//li[contains(.,'Surface :')]/text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(.,'Surface :')]/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        elif "m2" in desc:
            square_meters = desc.split("m2")[0].strip().split(" ")[-1].replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        energy_label = response.xpath("//span[@class='diagnostic-number']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", str(int(float(energy_label))))
        
        import dateparser
        from datetime import datetime
        available_date = response.xpath("//div[@class='entry-content']//p//text()[contains(.,'Disponible') or contains(.,'DISPONIBLE')]").get()
        if available_date:
            available_date = available_date.lower().replace("Disponible","").replace("disponible","").replace("le","")
            if "immédiatement" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d")) 
            else:
                if "(" in available_date:
                    available_date = available_date.split("(")[0].replace("début","").strip()
                available_date = available_date.replace("dès","").replace("au","").replace("à partir du","")
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Saint-Assiscle Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="33 0 4 68 54 92 34", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@sai.immo", input_type="VALUE")

        yield item_loader.load_item()