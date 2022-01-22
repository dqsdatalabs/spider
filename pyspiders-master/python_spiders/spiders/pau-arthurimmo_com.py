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
from  geopy.geocoders import Nominatim
import re 
 
class MySpider(Spider):
    name = 'pau-arthurimmo_com' 
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="PauArthurimmo_PySpider_france"
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    }
    # 1. FOLLOWING
    def start_requests(self):
        url="https://www.pau-arthurimmo.com/recherche,basic.htm?transactions=louer"
        yield Request(url=url,callback=self.parse,)
    def parse(self, response): 
        for item in response.xpath("//h2//a/@href").extract():
            f_url = response.urljoin(item) 
            yield Request(f_url,callback=self.populate_item,)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        if get_p_type_string(title): item_loader.add_value("property_type", get_p_type_string(title))
        else: return
        
        rent=response.xpath("//div[contains(@class,'text-4xl')]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].replace("\n","").strip().replace(" ",""))
        item_loader.add_value("currency","EUR")
        adres=response.xpath("//div[.='Quartier']/following-sibling::div/text()").get()
        if adres:
            item_loader.add_value("address",adres)

        room_count=response.xpath("//div[.='Nombre de pièces']/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//div[.='Nombre de salles de bain']/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        square_meters=response.xpath("//div[.='Surface habitable']/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        floor=response.xpath("//div[contains(.,'étages')]/following-sibling::div/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        description="".join(response.xpath("//div[@class='text-[#969A9D]']//p//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip().replace("\n",""))
            item_loader.add_value("description",description)
        external_id=response.xpath("//div[@class='text-gray-800']/text()").get()
        if external_id:
            externalid2=external_id.split("Référence")[-1].split("-")[0].replace("\n","")
            external_id1=external_id.split("Référence")[-1].split("-")[1].replace("\n","")
            item_loader.add_value("external_id",externalid2+"-"+external_id1)
        elevator=response.xpath("//div[.='Ascenseur']/following-sibling::div/text()").get()
        if elevator and "oui" in elevator.lower():
            item_loader.add_value("elevator",True)
        furnished=response.xpath("//div[.='Meublé']/following-sibling::div/text()").get()
        if furnished and "oui" in furnished.lower():
            item_loader.add_value("furnished",True)
        utilities=response.xpath("//div[.='Charges']/following-sibling::div/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].strip())
        images=response.xpath("//div[contains(@class,'relative')]//img//@src").getall()
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("city","PAU")
        available_date=response.xpath("//div[.='Date dispo']/following-sibling::div/text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date.split("T")[0])
        energy_label=response.xpath("//div[.='Consommations énergétiques']/following-sibling::div/img/@src").get()
        if energy_label:
            energy_label=energy_label.split("dpe?dpe=")[-1].split("&ges")[0]
            if energy_label:
                item_loader.add_value("energy_label",energy_label_calculate(int(float(energy_label.replace(",",".")))))
        item_loader.add_value("landlord_name","Arthurimmo.com Pau")
        item_loader.add_value("landlord_phone","05 59 00 09 23")
        item_loader.add_value("landlord_email","location@arthurimmo.com")
     

        
        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "appartement" in p_type_string.lower():
        return "apartment"
    elif p_type_string and "maison" in p_type_string.lower():
        return "house"
    else:
        return None