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
    name = 'lucilia_b_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://lucilia-b-immobilier.fr/r32/immobilier-Appartement-tours.html", "property_type": "apartment"},
	        {"url": "https://lucilia-b-immobilier.fr/r33/immobilier-Maison-tours.html", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//td[@width='572']//td[@height='150']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url,
                          callback=self.populate_item,
                          meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(.,'suivante')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page),
                          callback=self.parse, 
                          meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title)

        external_link=response.url
        if external_link and "contact.asp" in external_link.lower():
            return

        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Lucilia_B_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//td[contains(.,'Réf :')]/text()", input_type="M_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//td[contains(.,'LOYER/MOIS')]/text()", input_type="M_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        #ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="normalize-space(//h1[contains(@class,'typebien')]/text())", input_type="F_XPATH", replace_list={"Maison":"", "Appartement":""})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="normalize-space(//h1[contains(@class,'typebien')]/text())", input_type="F_XPATH", replace_list={"Maison":"", "Appartement":""})
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//p//text()[contains(.,'m²')]", input_type="F_XPATH", get_num=True, split_list={"m²":0, " ":-1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//p//text()[contains(.,'Libre le') or contains(.,'Disponible ') ]", input_type="F_XPATH", split_list={"le":1}, replace_list={".":""})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p//text()[contains(.,'garantie') or contains(.,'Garantie ')]", input_type="M_XPATH", get_num=True, split_list={"€":0, " ":-1})
        
        utilities = response.xpath("//p//text()[contains(.,'provision')]").get()
        if utilities and "€" in utilities:
            if "dont" in utilities:
                utilities = utilities.split("dont")[1].split("€")[0].strip()
                if " " in utilities:
                    utilities = utilities.split(" ")[-1]
            else:
                utilities = utilities.split("€")[0].strip().split(" ")[-1]
            item_loader.add_value("utilities", utilities)

        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//td[contains(@background,'image')]//@src", input_type="M_XPATH")
        
        energy_label = response.xpath("//div[@class='valeur']//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        desc = " ".join(response.xpath("//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "studio" in title.lower():
            item_loader.add_value("room_count", "1")
        elif "chambres" in desc:
            room_count = desc.split("chambres")[0].strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
        elif 'deux chambre' in desc.lower():
            item_loader.add_value("room_count", "2")
        elif response.xpath("//h1/text()").get():
            item_loader.add_value("room_count", "1")

        parking = response.xpath("//tr/td/p[contains(.,'garage')]/text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Lucilia B. Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="02.47.66.05.66", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@luciliab-immobilier.fr", input_type="VALUE")

        yield item_loader.load_item()