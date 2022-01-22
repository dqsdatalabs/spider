# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re
from datetime import datetime 
class MySpider(Spider):
    name = 'jlw_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):

        start_urls = [
            {
                "type" : 2,
                "property_type" : "house"
            },
            {
                "type" : 1,
                "property_type" : "apartment"
            },
            
        ] #LEVEL-1

        for url in start_urls:
            r_type = str(url.get("type"))
            payload = {
                "typetransaction": "Location",
                "reference": "",
                "npieces": "",
                "nchambres": "",
                "typebien": r_type,
                "localisationville": "",
                "localisationsecteur": "",
                "budgetmax": "",
            }

            yield FormRequest(url="https://jlw.fr/bien-immobilier",
                                callback=self.parse,
                                formdata=payload,
                                #headers=self.headers,
                                meta={'property_type': url.get('property_type')})
            
    # 1. FOLLOWING
    def parse(self, response): 
        for item in response.xpath("//script[contains(.,'Product')]/text()").extract():
            data = json.loads(item)
            f_url = data["url"]
            images = data["image"]
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type"), "images":images},
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("images", response.meta.get('images'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Jlw_PySpider_" + self.country + "_" + self.locale)

        item_loader.add_xpath("external_id", "//div/div[contains(.,'Référence')]/following-sibling::div/text()[normalize-space()]")
        title = response.xpath("//h2[@class='single-product-title']//text()").extract_first()
        item_loader.add_value("title", title)

        rent = response.xpath("//div[@class='single-product-price']/text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ","."))

        room_count = response.xpath("//div/div[contains(.,'Pièces')]/following-sibling::div/text()[normalize-space()]").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.split("pièce")[0].strip())

        deposit = response.xpath("//div/div[contains(.,'Dépôt de garantie')]/following-sibling::div/text()[normalize-space()]").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].replace(" ","").strip())
        utilities = response.xpath("//div/div[contains(.,'Charges')]/following-sibling::div/text()[normalize-space()]").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].replace(" ","").strip())
        
        address = response.xpath("//li[@itemprop='itemListElement'][2]/a/text()").extract_first()
        if address:
            zipcode = [x for x in address.split(" ") if x.isdigit()]
            if zipcode:
                item_loader.add_value("zipcode",zipcode)
                item_loader.add_value("address", address.split(str(zipcode[0]))[1].strip().capitalize())
                item_loader.add_value("city", address.split(str(zipcode[0]))[1].strip().capitalize())

        desc = "".join(response.xpath("//div[@class='single-product-description']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(m²|meters2|metres2|meter2|metre2|mt2|m2|M2)",desc.replace(",","."))
            if unit_pattern:
                sq=int(float(unit_pattern[0][0]))
                item_loader.add_value("square_meters", str(sq))
            else:
                unit_pattern_title = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(m²|meters2|metres2|meter2|metre2|mt2|m2|M2|M²)",title.replace(",","."))
                if unit_pattern_title:
                    square_title=unit_pattern_title[0][0]
                    sq=int(float(square_title))
                    item_loader.add_value("square_meters", str(sq))

            datetimeobject = False
            if "Disponible le" in desc:
                date = desc.split("Disponible le ")[1].split(".")[0].strip()
                if date:
                    try:
                        datetimeobject = datetime.strptime(date,'%d/%m/%Y')
                    except:
                        date = date.split("Loyer")[0].replace("\n","").strip()
                    if datetimeobject:
                        newformat = datetimeobject.strftime('%Y-%m-%d')
                        item_loader.add_value("available_date",newformat)

        energy_label = response.xpath("//div[@class='conso__dpe']/div[contains(@class,'conso__label')]/@class").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("--")[1].upper() )
      

        item_loader.add_xpath("landlord_email", "//div[@class='agent-meta']//li[contains(.,'e-mail')]/a/text()")
        item_loader.add_xpath("landlord_name", "//div[@class='agent-name']//text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='agent-meta']//li[contains(.,'tél.')]/a/text()")
        yield item_loader.load_item()
