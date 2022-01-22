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
import dateparser
class MySpider(Spider):
    name = 'chantonnay_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    start_urls = ["https://www.chantonnay-immobilier.com/location.htm"]
    external_source='Chantonnayimmobilier_PySpider_france_fr'
    
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'art-postcontent')]/div[contains(@class,'art-content-layout')]"):
            f_url = response.urljoin(item.xpath(".//a[contains(@href,'L')]/@href").get())
            prop_type = item.xpath(".//p[contains(.,'€')]/span/span[1]/text()").get()
            if prop_type and "APPARTEMENT" in prop_type:
                prop_type = "apartment"
                yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : prop_type},)
            elif prop_type and "MAISON" in prop_type:
                prop_type = "house"
                yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : prop_type},)
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        prop_type = response.meta.get('property_type')
        item_loader.add_value("property_type", prop_type)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
       
        title = " ".join(response.xpath("//h1//text()").extract())
        item_loader.add_value("title", re.sub("\s{2,}", " ", title.replace("\n","")).split("(H.A.I)"))

        item_loader.add_xpath("external_id", "//p[contains(.,'Référence')]/span[1]/text()")

        price = response.xpath("//div[@class='art-layout-cell']/h1/span[3]/text()").extract_first()
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))

        room_count = response.xpath("//p[contains(.,'pièce')]/span[3]/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        square = response.xpath("//p[contains(.,'Référence')]/span[2]/text()").extract_first()
        if square:
            square_meters = square.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        desc = "".join(response.xpath("//div[@class='art-layout-cell']/p[last()]/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "Dépôt garantie :" in desc:
                deposit=desc.split("Dépôt garantie :")[1].split("EUR")[0]
                item_loader.add_value("deposit", deposit.strip())
            if "Honoraires agence" in desc:
                utilities=desc.split("Honoraires agence")[1].split("EUR")[0]
                item_loader.add_value("utilities", utilities.strip())
            if "Disponible au" in desc:
                try:           
                    available_date = desc.split("Disponible au")[1].split(".")[0]     
                    date_parsed = dateparser.parse(available_date, languages=['fr'])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
                except:
                    pass
       
        item_loader.add_xpath("city","//div[@class='art-layout-cell']/h1/span[2]/text()")
        item_loader.add_xpath("address","//div[@class='art-layout-cell']/h1/span[2]/text()")

        images = [response.urljoin(x) for x in response.xpath("//div[@class='soliloquy-outer-container']//li/img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_email", "contact@chantonnay-immobilier.com")
        item_loader.add_value("landlord_name", "Cabinet Fruchet")
        item_loader.add_value("landlord_phone", "02 51 94 54 30")
        yield item_loader.load_item()

