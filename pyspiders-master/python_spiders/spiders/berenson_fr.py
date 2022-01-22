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
    name = 'berenson_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.berenson.fr/search.php?a=2&b%5B%5D=appt&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&ajax=1&facebook=1&start=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.berenson.fr/search.php?a=2&b%5B%5D=house&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&ajax=1&facebook=1&start=0",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 12)
        seen = False

        for item in response.xpath("//div[@id='result']/div[contains(@class,'res')]"):
            seen = True
            follow_url = response.urljoin(item.xpath("./div/a/@href").get())
            is_rented = item.xpath(".//div[@class='band_rotate' and contains(.,'Loué')]").get()
            if not is_rented: yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 12 or seen:
            yield Request(response.url.replace("&start=" + str(page - 12), "&start=" + str(page)), callback=self.parse, meta={'property_type':response.meta["property_type"], 'page':page+12})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Berenson_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='tech_detail']//td[contains(.,'Référence')]/following-sibling::td/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='tech_detail']//td[contains(.,'Ville')]/following-sibling::td//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='tech_detail']//td[contains(.,'Ville')]/following-sibling::td//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[@class='tech_detail']//td[contains(.,'Ville')]/following-sibling::td/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@class='tech_detail']//td[.='Surface']/following-sibling::td/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        
        if response.xpath("//div[@class='tech_detail']//td[.='Chambres']/following-sibling::td/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='tech_detail']//td[.='Chambres']/following-sibling::td/text()", input_type="F_XPATH", get_num=True)
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='tech_detail']//td[.='Pièces']/following-sibling::td/text()", input_type="F_XPATH", get_num=True)
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='tech_detail']//td[.='Salle de bains']/following-sibling::td/text()", input_type="F_XPATH", get_num=True)
        
        rent = response.xpath("//div[@class='basic_copro']/text()[contains(.,'Loyer CC :')]").get()
        if rent:
            price = rent.split("Loyer CC :")[1].split("€")[0].replace(" ","").replace(",",".")
            item_loader.add_value("rent", int(float(price)))
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='basic_copro']/text()[contains(.,'garantie')]", input_type="F_XPATH", get_num=True, split_list={"garantie":1, "€":0}, replace_list={" ":""})
        
        desc = " ".join(response.xpath("//div[@id='details']//text()").getall())
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
        
        if "terrasse" in response.xpath("//h1/text()").get().lower():
            item_loader.add_value("terrace", True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='layerslider']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'setView')]/text()", input_type="F_XPATH", split_list={"setView([":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'setView')]/text()", input_type="F_XPATH", split_list={"setView([":1,",":1,"]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@class='tech_detail']//td[.='Ameublement']/following-sibling::td/text()[not(contains(.,'Non'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[@class='tech_detail']//td[.='Ascenseur']/following-sibling::td/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@class='tech_detail']//td[.='Stationnement int.']/following-sibling::td/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//ul[@class='ul_plus']/li[contains(.,'balcon') or contains(.,'Balcon')]/text()", input_type="F_XPATH", tf_item=True)
        
        energy_label = response.xpath("//b[@class='dpe-letter-active']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[0].strip())

        if "\u00e9tage" in desc:
            floor = desc.split("\u00e9tage")[0].strip().split(" ")[-1].replace("ème","").replace("em","").replace("er","").replace("e","")
            if floor.isdigit():
                item_loader.add_value("floor", floor)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Berenson Real Estate", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01 58 37 37 37", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@berenson.fr", input_type="VALUE")

        yield item_loader.load_item()