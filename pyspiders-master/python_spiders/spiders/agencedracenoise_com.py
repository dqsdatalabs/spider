# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider  
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader  
import json

class MySpider(Spider):
    name = 'agencedracenoise_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Agencedracenoise_PySpider_france"
    def start_requests(self): 
        start_urls = [
            {
                "url" : [
                    "https://www.agencedracenoise.com/rechercher?category=2&type=5&city=&mandat=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agencedracenoise.com/rechercher?category=2&type=18&city=&mandat=",
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

        for item in response.xpath("//a[@class='property']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page: yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        # ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agencedracenoise_PySpider_france", input_type="VALUE")
        item_loader.add_value("external_source",self.external_source)
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[contains(@class,'upper left')]/text() | //div/span[contains(.,'Région ')]/following-sibling::text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[contains(@class,'upper left')]/text()[contains(.,'(')]", input_type="F_XPATH", split_list={"(":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div/span[contains(.,'Ville')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h2/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[@itemprop='description']//text()", input_type="M_XPATH")
        
        if response.xpath("//div/span[contains(.,'Surface')]/following-sibling::text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div/span[contains(.,'Surface')]/following-sibling::text()", input_type="F_XPATH", get_num=True, split_list={"m":0})
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//p[@itemprop='description']//text()[contains(.,'m²')]", input_type="F_XPATH", get_num=True, split_list={"m²":0, " ":-1, ".":0})
            
        if response.xpath("//div/span[contains(.,'Chambre')]/following-sibling::text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div/span[contains(.,'Chambre')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div/span[contains(.,'Pièce')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
            

        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div/span[contains(.,'Salle')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']/span/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        deposit=response.xpath("//div[@class='legal']/text()").getall()
        if deposit:
            for i in deposit: 
                if "dépôt" in i.lower():
                    item_loader.add_value("deposit",i.split(":")[-1].split("€")[0])
                if "charges" in i.lower():
                    item_loader.add_value("utilities",i.split(":")[-1].split("€")[0])
        
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={"-":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'gallery')]//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div/span[contains(.,'étages')]/following-sibling::text()", input_type="F_XPATH", replace_list={":":""})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div[@class='legal']//text()[contains(.,'€')]", input_type="F_XPATH", get_num=True, split_list={"€":0," ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div/span[contains(.,'Parking')]/following-sibling::text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div/span[contains(.,'Ascenseur')]/following-sibling::text()[contains(.,'oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Agence DRACENOISE", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 94 68 07 66", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@agencedracenoise.com", input_type="VALUE")
        
        energy_label = response.xpath("//div[@class='dpe']/div[@class='letter']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        yield item_loader.load_item()