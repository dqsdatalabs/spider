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
    name = 'agence_grolleau_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_url = "https://www.agence-grolleau.com/location-annee-vendee/"
        yield Request(start_url, callback=self.parse)   

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'annonces-list')]/div//p[@class='titre-annonce']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)

        next_button = response.xpath("//a[@class='next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.xpath("//div[contains(text(),'Type de bien')]/following-sibling::div/text()").get()
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        else: return
        item_loader.add_value("external_link", response.url)

        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agence_Grolleau_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='lbl'][contains(.,'Ville')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='lbl'][contains(.,'Ville')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@class='lbl'][contains(.,'habitable')]/following-sibling::div/text()", input_type="F_XPATH", split_list={"m":0})
        
        if response.xpath("//div[@class='lbl'][contains(.,'Chambre')]/following-sibling::div/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='lbl'][contains(.,'Chambre')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='lbl'][contains(.,'pièce')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='lbl'][contains(.,'Salle')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        
        zipcode = "".join(response.xpath("//div[contains(@class,'titre-single')]/text()").getall())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split("/")[-1].strip())
        
        energy_label = response.xpath("//div[@class='lbl'][contains(.,'DPE')]/following-sibling::div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
            
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[@class='lbl'][contains(.,'Ascenseur')]/following-sibling::div/text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='prix']/text()", input_type="F_XPATH", get_num=True, split_list={":":1, "€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        
        desc = " ".join(response.xpath("//div[contains(@class,'desc-content')]//p/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        import dateparser
        if "Disponible le" in desc:
            available_date = desc.split("Disponible le")[1].split("Loyer")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
                
        images =response.xpath("//div[@class='carousel-inner']//@style").getall()
        for i in range(1,len(images)):
            item_loader.add_value("images", images[i].split("url(")[1].split(")")[0])
        
        if "garantie" in desc:
            deposit = desc.lower().split("garantie")[1].split("?")[0].replace(":","").strip().split(" ")[0].strip()
            item_loader.add_value("deposit", int(float(deposit)))
        
        if "? et " in desc:
            utilities = desc.lower().split("? et ")[1].split("?")[0].strip()
            item_loader.add_value("utilities", int(float(utilities)))
        
        item_loader.add_value("external_id", response.url.split("id=")[1])
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="AGENCE GROLLEAU", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[contains(@class,'annonceur')]//p/strong/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="grolleau.angles@imandc.fr", input_type="VALUE")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "appartement" in p_type_string.lower():
        return "apartment"
    elif p_type_string and "maison" in p_type_string.lower():
        return "house"
    else:
        return None