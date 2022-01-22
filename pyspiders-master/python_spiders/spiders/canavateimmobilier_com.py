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
import dateparser

class MySpider(Spider):
    name = 'canavateimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "https://www.canavateimmobilier.com/fr/recherche/"
    current_index = 0
    other_prop = ["2"]
    other_prop_type = ["house"]
    def start_requests(self):
        formdata = {
            "nature": "2",
            "type[]": "1",
            "price": "",
            "age": "",
            "tenant_min": "",
            "tenant_max": "",
            "rent_type": "",
            "newprogram_delivery_at": "",
            "newprogram_delivery_at_display": "",
            "currency": "EUR",
            "customroute": "",
            "homepage": "",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            formdata=formdata,
            meta={
                "property_type":"apartment",
            }
        )


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        for item in response.xpath("//div[@class='buttons']//a[@class='button']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        next_page = response.xpath("//li[@class='nextpage']/a/@href").get()
        if next_page:
            p_url = f"https://www.canavateimmobilier.com/fr/recherche/{page}"
            yield Request(p_url, dont_filter=True, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "nature": "2",
                "type[]": self.other_prop[self.current_index],
                "price": "",
                "age": "",
                "tenant_min": "",
                "tenant_max": "",
                "rent_type": "",
                "newprogram_delivery_at": "",
                "newprogram_delivery_at_display": "",
                "currency": "EUR",
                "customroute": "",
                "homepage": "",
            }
            yield FormRequest(
                url=self.post_url,
                callback=self.parse,
                dont_filter=True,
                formdata=formdata,
                meta={
                    "property_type":self.other_prop_type[self.current_index],
                }
            )
            self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Canavateimmobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//article/div/h2/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//article/div/h2/text()[2]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//article/div/h2/text()[2]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//article/div//li[contains(.,'Ref')]/text()", input_type="F_XPATH", split_list={".":1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//article/div//li[contains(.,'Mois')]/text()", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(.,'Surface')]/span/text()", input_type="F_XPATH", get_num=True, split_list={"m":0})
        
        if response.xpath("//li[contains(.,'Chambre')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Chambre')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        elif response.xpath("//li[contains(.,'Pièce')]/span/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Pièce')]/span/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'salle')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})    
        available_date = response.xpath("//li[contains(.,'Disponible ')]/span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"], languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%d-%m")
                item_loader.add_value("available_date", date2)
        
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'Garage')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Meublé')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Ascenseur')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//li[contains(.,'Lave-vaisselle')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//li[contains(.,'Lave-linge')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges')]/span/text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'garantie')]/span/text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'L.marker([')]/text()", input_type="F_XPATH", split_list={"L.marker([":2, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'L.marker([')]/text()", input_type="F_XPATH", split_list={"L.marker([":2, ",":1, "]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//p[contains(@class,'userName')]/strong/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//span[contains(@class,'phone ')]/a/text()", input_type="F_XPATH", replace_list={"+":""})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//span[contains(@class,'mail')]/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'show-carousel')]//@src", input_type="M_XPATH")
        
        floor = response.xpath("//li[contains(.,'Etage')]/span/text()").get()
        if floor:
            if "/" in floor:
                item_loader.add_value("floor", floor.split("/")[0].strip())
            else:
                item_loader.add_value("floor", floor.strip())
        
        energy_label = response.xpath("//img/@src[contains(.,'diagnostic') and contains(.,'1')]").get()
        if energy_label:
            energy_label = energy_label.split("/")[-1]
            if "-" not in energy_label:
                item_loader.add_value("energy_label", energy_label)
        
        desc = " ".join(response.xpath("//p[@id='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        yield item_loader.load_item()