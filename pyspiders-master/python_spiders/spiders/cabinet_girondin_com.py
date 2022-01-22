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
    name = 'cabinet_girondin_com'
    execution_type='testing'
    country='france'
    locale='fr' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.cabinet-girondin.com/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=1&saisie=O%c3%b9+d%c3%a9sirez-vous+habiter+%3f&tri=d_dt_crea&ANNLISTEpg=1&",
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

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@id='recherche-resultats-listing']/div/div/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        if page == 2 or seen:
            follow_url = f"http://www.cabinet-girondin.com/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=1&saisie=O%c3%b9+d%c3%a9sirez-vous+habiter+%3f&tri=d_dt_crea&ANNLISTEpg={page}&"
            yield Request(follow_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1].split(".")[0])
        
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Cabinet_Girondin_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1//text()[2]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1//text()[2]", input_type="F_XPATH", split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1//text()[2]", input_type="F_XPATH", split_list={"(":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@class='span12']/div//div//text()[contains(.,'m²')]", input_type="F_XPATH", get_num=True, split_list={",":0})

        if response.xpath("//div[@class='span12']/div//div//text()[contains(.,'chambre')]").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='span12']/div//div//text()[contains(.,'chambre')]", input_type="F_XPATH", get_num=True, split_list={" ":0})
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='span12']/div//div//text()[contains(.,'pièce')]", input_type="F_XPATH", get_num=True, split_list={" ":0})
        
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="normalize-space(//span[@itemprop='price']/text())", input_type="F_XPATH", get_num=True, replace_list={"\xa0":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//p[@class='dt-dispo']/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//strong[contains(.,'garantie')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1, "€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1, "€":0})
        
        desc = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "salle de bains" in desc:
            bathroom_count = desc.split("salle de bains")[0].strip().split(" ")[-1]
            if "une" in bathroom_count:
                item_loader.add_value("bathroom_count", "1")

        furnished = " ".join(response.xpath("//div[@class='span12']/ul/li[div[.='Meublé']]/div[2]/text()").getall())
        if furnished:
            if "oui" in furnished:
                item_loader.add_value("furnished", True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='slider']//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LONGITUDE')]/text()", input_type="F_XPATH", split_list={'PUBLICLAT: "':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LONGITUDE')]/text()", input_type="F_XPATH", split_list={'PUBLICLNG: "':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="CABINET GIRONDIN", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="05 56 79 01 79", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@cabinet-girondin.com", input_type="VALUE")
        
        yield item_loader.load_item()