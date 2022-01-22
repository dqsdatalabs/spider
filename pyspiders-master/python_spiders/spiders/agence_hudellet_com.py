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
    name = 'agence_hudellet_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agence-hudellet.com/annonces-immobilier-location/appartement-{}",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agence-hudellet.com/annonces-immobilier-location/maison-villa-{}",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@class='c-info']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agence_Hudellet_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//b[contains(.,'Référence')]//parent::div//text()", input_type="M_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1//text()", input_type="M_XPATH", split_list={"-":1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'desc')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//b[contains(.,'Surface ')]//parent::div//text()", input_type="M_XPATH", get_num=True, split_list={":":1,"m":0,".":0})
        if response.xpath("//b[contains(.,'chambres')]//following-sibling::text()[not(contains(.,'0'))]").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//b[contains(.,'chambres')]//following-sibling::text()[not(contains(.,'0'))]", input_type="F_XPATH", get_num=True)
        elif response.xpath("//b[contains(.,'pièces')]//following-sibling::text()[not(contains(.,'0'))]").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//b[contains(.,'pièces')]//following-sibling::text()[not(contains(.,'0'))]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'title')]//div[contains(.,'€/mois')]//text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//b[contains(.,'garantie')]//parent::div//text()", input_type="M_XPATH", get_num=True, split_list={":":1,"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//b[contains(.,'Provision pour charges')]//following-sibling::text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'c-product-gallery-content')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="AGENCE HUDELLET", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 68 35 21 19", input_type="VALUE")
        
        city_zipcode = "".join(response.xpath("//h1//text()").getall())
        if city_zipcode:
            zipcode = city_zipcode.split("-")[1].strip().split(" ")[-1]
            city = city_zipcode.split("-")[1].strip().split(zipcode)[0].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        yield item_loader.load_item()