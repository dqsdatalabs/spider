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
    name = 'cabinet_patrice_ryaux_com'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings ={
        "PROXY_ON":"True"
        }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.cabinet-patrice-ryaux.com/annonces/search.php?cat=1&loc=&action=send&type=1", "property_type": "apartment"},
	        {"url": "https://www.cabinet-patrice-ryaux.com/annonces/search.php?cat=11&loc=&action=send&type=1", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='annonce-item']//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page),
                          callback=self.parse,
                          meta={"property_type":response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Cabinet_Patrice_Ryaux_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[contains(@class,'annonce-reference')]//text()", input_type="F_XPATH", split_list={".":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//i[contains(@class,'icon-location')]//parent::span//text()", input_type="F_XPATH", lower_or_upper=1)
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//i[contains(@class,'icon-location')]//parent::span//text()", input_type="F_XPATH", lower_or_upper=1)
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[contains(@class,'heading')]//h1//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'annonce-description')]//text()", input_type="M_XPATH")
        if response.xpath("//span[contains(.,'chambres')]//following-sibling::span//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'chambres')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//span[contains(.,'pièces')]//following-sibling::span//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'pièces')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[contains(.,'Surface habitable')]//following-sibling::span//text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='annonce-prix']//text()", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        # ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='annonce-prix-conditions']//text()", input_type="F_XPATH", get_num=True,split_list={"garantie:":1,"€":0,".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[contains(.,'charges')]//text()", input_type="F_XPATH", get_num=True, split_list={"€":0," ":-1,".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//span[contains(.,'performance énergetique')]//following-sibling::span//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'slide')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[contains(@class,'adresse')]//span[contains(@class,'name')]//text()", input_type="F_XPATH", replace_list={"/":""})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[contains(@class,'nego')]//span[2]//text()", input_type="F_XPATH")
        
        # Honoraire: 819.39 € - Dépôt de garantie: 640.00 €
        # Honoraire: 825.00 € -
        deposit = response.xpath("//div[@class='annonce-prix-conditions']/text()").get()
        deposit = deposit.split(":")[1].split("€")[0].strip(" ").split(".")[0]
        if deposit:
            item_loader.add_value("deposit",deposit)

        landlord_phone = response.xpath("//div[contains(@class,'nego')]//span[2]//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", "0380250505")
        landlord_email = response.xpath("//div[contains(@class,'nego')]//a[contains(@class,'email')]//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        else:
            name = response.xpath("//div[contains(@class,'adresse')]//span[contains(@class,'name')]//text()").get()
            if name and "Dijon" in name:
                item_loader.add_value("landlord_email","dijon@ryaux.fr")
            elif name and "Beaune" in name:
                item_loader.add_value("landlord_email","agence@ryaux.fr")
            elif name and "Chalon sur Saône" in name:
                item_loader.add_value("landlord_email","chalon@ryaux.fr")
            elif name and "Tournus" in name:
                item_loader.add_value("landlord_email","tournus@ryaux.fr")
            elif name and "Chagny" in name:
                item_loader.add_value("landlord_email","chagny@ryaux.fr")
        
        yield item_loader.load_item()