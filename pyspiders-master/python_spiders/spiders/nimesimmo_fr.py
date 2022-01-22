# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy
import dateparser

class MySpider(Spider):
    name = 'nimesimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    start_urls = ["https://www.nimesimmo.fr/catalog/advanced_search_result.php?action=update_search&search_id=&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2%2C1&C_27_tmp=2&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=&C_94_type=NUMBER&C_94_search=COMPRIS&C_94_MIN="]
    def parse(self, response):

        for item in response.xpath("//div[@class='product-listing']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
        
        next_button = response.xpath("//li[contains(@class,'next-link') and contains(@class,'active')]/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)      
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        prop_type = response.xpath("//ol[@class='breadcrumb']/li[2]//span/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else:
            return    
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Nimesimmo_PySpider_france")  
        item_loader.add_xpath("title","//h1//text()")       
        item_loader.add_xpath("external_id", "substring-after(//div[@class='product-ref']/text(),': ')")
        room_count = response.xpath("//li/div[div[.='pièces']]/div[2]/b/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//li/div[div[.='Nombre pièces']]/div[2]/b/text()")

        bathroom_count = response.xpath("//li/div[div[contains(.,'Salle(s) d')]]/div[2]/b/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        address = response.xpath("//div[@class='product-localisation']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split(" ")[0])
            item_loader.add_value("city"," ".join(address.split(" ")[1:]))
  
        item_loader.add_xpath("floor", "//li/div[div[.='Etage']]/div[2]/b/text()")
        square_meters = response.xpath("//li/div[div[.='Surface']]/div[2]/b/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.split("m")[0].strip())))
        parking = response.xpath("//li/div[div[contains(.,' parking')]]/div[2]/b/text()").get()
        if parking:
            if "non" in parking.lower() or "0" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        furnished = response.xpath("//li/div[div[.='Meublé']]/div[2]/b/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        terrace = response.xpath("//li/div[div[contains(.,' terrasses')]]/div[2]/b/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        balcony = response.xpath("//li/div[div[.='Nombre balcons']]/div[2]/b/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        elevator = response.xpath("//li/div[div[.='Ascenseur']]/div[2]/b/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        energy_label = response.xpath("//li/div[div[.='Conso Energ']]/div[2]/b/text()[not(contains(.,'Non')) and .!='Vierge']").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
     
        description = " ".join(response.xpath("//div[@class='product-description']/text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider_product']/div/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        available_date = response.xpath("//div[@class='product-description']/text()[contains(.,'Disponible le')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Disponible le")[-1].strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        rent = response.xpath("//div[@class='product-price']//span[@class='alur_loyer_price']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace("\xa0",".").replace(" ",""))
        deposit = response.xpath("//li/div[div[.='Dépôt de Garantie']]/div[2]/b/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ",""))
        item_loader.add_xpath("utilities", "//li/div[div[.='Provision sur charges']]/div[2]/b/text()")
        item_loader.add_value("landlord_name", "NIMES IMMOBILIER")
        item_loader.add_value("landlord_phone", "04.66.67.11.50")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "commercial" in p_type_string.lower():
        return None
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "maison" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    else:
        return None