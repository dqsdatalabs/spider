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
    name = 'faivre_immo_net'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Faivre_Immo_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.faivre-immo.com/advanced-search/page/1/?filter_search_action%5B0%5D=location&filter_search_type%5B0%5D=appartement&advanced_area&nombre-de-pieces&superficie-maximum&code-postal&prix-maximum",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.faivre-immo.com/advanced-search/page/1/?filter_search_action%5B%5D=location&filter_search_type%5B%5D=maisonvilla&advanced_area=&nombre-de-pieces=&superficie-maximum=&code-postal=&prix-maximum=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'listing-read-more')]/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        next_button = response.xpath("//li[@class='roundright']/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//h1//text()[contains(.,'Ref')]", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'details')]//span[contains(@class,'price_label_before')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[contains(@class,'details')]/span[contains(@style,'#000')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[contains(@class,'details')]/span[contains(@style,'#000')]/text()", input_type="F_XPATH", split_list={"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[contains(@class,'details')]/span[contains(@style,'#000')]/text()", input_type="F_XPATH", split_list={"(":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[contains(.,'Surface')]/following-sibling::text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ",":0})
        
        if response.xpath("//span[contains(.,'Chambre')]/following-sibling::text()[.!='0']").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'Chambre')]/following-sibling::text()[.!='0']", input_type="F_XPATH", get_num=True)
        elif response.xpath("//span[contains(.,'Pièce')]/following-sibling::text()[.!='0']").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'Pièce')]/following-sibling::text()[.!='0']", input_type="F_XPATH", get_num=True)
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'Salle')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//span[contains(.,'Etage')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//h1[contains(.,'Meublé')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//span[contains(.,'Balcon')]/following-sibling::text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//span[contains(.,'Parking')]/following-sibling::text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'item')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Faivre Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="05 61 62 17 09", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@faivre-immo.com", input_type="VALUE")

        desc = " ".join(response.xpath("//div[contains(@class,'property_custom_detail')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        prop_type = response.xpath("//h1//text()").get()
        if "studio" in prop_type.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta["property_type"])
        
        import dateparser
        if "disponible le" in desc.lower():
            available_date = desc.lower().split("disponible le")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        yield item_loader.load_item()