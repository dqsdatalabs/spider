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
    name = 'ca_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    url = "https://www.ca-immobilier.fr/louer/recherche/resultat"
    formdata = {
        'from': '0',
        'sections[]': 'location',
        'types[]': '',
        'distance': '0',
        'displayMode': 'mosaic',
        'size': '20'
    }
    headers = {
        'Connection': 'keep-alive',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://www.ca-immobilier.fr',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://www.ca-immobilier.fr/louer/recherche?sortby=&codes=&sections=location&types=appartment&zones=&distance=0&displayMode=mosaic',
        'Accept-Language': 'tr,en;q=0.9'
    }

    def start_requests(self):
        property_types = {"apartment": "appartment", "house": "house"}
        for k, v in property_types.items():
            self.formdata["types[]"] = v
            yield FormRequest(self.url, headers=self.headers, formdata=self.formdata, dont_filter=True, callback=self.parse, meta={'property_type': k, 'formdata_type': v})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 1)
        seen = False

        data = json.loads(response.body)
        for item in data["hits"]["hits"]:
            seen = True
            follow_url = "https://www.ca-immobilier.fr/biens/location/" + item["base_origin"] + "/" + item["slug"]
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 1 or seen:
            self.formdata["types[]"] = response.meta["formdata_type"]
            self.formdata["from"] = str(page)
            yield FormRequest(self.url, 
                            headers=self.headers, 
                            formdata=self.formdata, 
                            dont_filter=True, 
                            callback=self.parse, 
                            meta={'property_type': response.meta["property_type"], 'formdata_type': response.meta["formdata_type"], 'page':page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Ca_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//article/h2[@itemprop='title']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']//strong/text()", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        
        square_meters = response.xpath("//li/span[contains(.,'habitable')]/strong/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(" ")[0]
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room=response.xpath("//span[contains(.,'Nombre de pièces : ')]/text()/following-sibling::strong/text()").get()
        if room:
            item_loader.add_value("room_count",room)
            
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li/span[contains(.,'Salle')]/strong/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li/span[contains(.,'Étage')]/strong/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li/span[contains(.,'Disponible')]/strong/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li/span[contains(.,'balcon')]/strong/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li/span[contains(.,'Terrasse')]/strong/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li/span[contains(.,'parking')]/strong/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        
        deposit = response.xpath("//li/span[contains(.,'garantie')]/strong/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip()
            item_loader.add_value("deposit", int(float(deposit)))
            
        energy_label = response.xpath("//div[contains(@class,'indicator-')]/p[2]/text()").get()
        if energy_label and energy_label.replace(".","").isdigit():
            item_loader.add_value("energy_label", str(int(float(energy_label))))
        
        desc = " ".join(response.xpath("//div[contains(@class,'details-description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.split("location")[1].replace("-","").strip())
            
        images = [response.urljoin(x.split("url(")[1].split(")")[0]) for x in response.xpath("//div[contains(@class,'slick__item')]//@style").getall()]
        if images:
            item_loader.add_value("images", images)
            
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//li/span[contains(.,'Piscine')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li/span[contains(.,'Ascenseur')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li/span[contains(.,'Meublé')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng:')]/text()", input_type="F_XPATH", split_list={"lat:":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng:')]/text()", input_type="F_XPATH", split_list={"lng:":1," ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Crédit Agricole Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="05 34 40 17 98", input_type="VALUE")

        city = response.xpath("//script[contains(.,'bien_loc_city')]/text()").get()
        if city: item_loader.add_value("city", city.split('bien_loc_city: "')[-1].split('"')[0].strip())

        utilities = response.xpath("//span[contains(text(),'provisions sur charges')]/text()").get()
        if utilities: item_loader.add_value("utilities", utilities.split('dont ')[1].split('€')[0].strip())
        
        yield item_loader.load_item()