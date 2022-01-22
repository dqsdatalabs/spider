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
    name = 'ipfinance_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        yield Request("https://www.ipfinance.fr/location-gestion-gard/", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='liste_biens']/div"):
            property_type = item.xpath(".//div[@class='description']/text()").get()
            follow_url = item.xpath(".//div[@class='bouton-gris']/a/@href").get()
            if property_type:
                if get_p_type_string(property_type):
                    yield Request(response.urljoin(follow_url), callback=self.populate_item, meta={"property_type":get_p_type_string(property_type)})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Ipfinance_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//td[contains(.,'Référence')]/following-sibling::td/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//td[contains(.,'Localité')]/following-sibling::td/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//td[contains(.,'Localité')]/following-sibling::td/text()", input_type="F_XPATH", split_list={"(":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//td[contains(.,'Localité')]/following-sibling::td/text()", input_type="F_XPATH", split_list={"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h2/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//td[contains(.,'habitable')]/following-sibling::td//text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//td[contains(.,'chambres')]/following-sibling::td//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'prix')]/text()", input_type="F_XPATH", get_num=True, split_list={"€":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//td[contains(.,'étages ')]/following-sibling::td//text()", input_type="F_XPATH")
    
        desc = " ".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x.split("url(")[1].split(")")[0] for x in response.xpath("//div/@style[contains(.,'url(')]").getall()]
        if images:
            item_loader.add_value("images", images)
        elif response.xpath("//div[@class='photo-fiche']//@src").getall():
            item_loader.add_xpath("images", "//div[@class='photo-fiche']//@src")
        
        energy_label = response.xpath("//div[@class='dpe']/div[contains(@class,'indice')]/@class").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(" ")[-1].upper())
        
        import dateparser
        if "disponible le " in desc:
            available_date = desc.split("disponible le")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="IP FINANCE", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 66 21 05 65", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="gestion@ipfinance.fr", input_type="VALUE")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "t1" in p_type_string.lower() or "t2" in p_type_string.lower() or "t3" in p_type_string.lower() or "t4" in p_type_string.lower() or "t5" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None