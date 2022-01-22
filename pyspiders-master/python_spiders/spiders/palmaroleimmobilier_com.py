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
    name = 'palmaroleimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        url = "http://www.palmaroleimmobilier.com/fr/locations"
        yield Request(url, callback=self.parse)

    def parse(self, response):

        for item in response.xpath("//li[contains(@class,'ad')]"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'Vue détaillée')]/@href").get())
            property_type = item.xpath(".//h2/text()").get()
            if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
        
        next_button = response.xpath("//li[@class='nextpage']/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Palmaroleimmobilier_PySpider_france")       
        item_loader.add_xpath("title", "//h1//text()")
        item_loader.add_xpath("external_id", "substring-after(//li[contains(.,'Ref.')]/text(),'Ref. ')")
        room_count = response.xpath("//li[contains(.,'chambre')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("chambre")[0])
        else:
            room_count = response.xpath("//li/span[contains(.,'pièce')]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split("pièce")[0])
        bathroom_count = response.xpath("//li[contains(.,'salle de bain')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("salle ")[0])
    
        address = response.xpath("//section[@class='showPictures']//h2/text()[last()]").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split("Centre")[0].strip())
        
        square_meters = response.xpath("//li[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters.replace(",","."))))
        
        floor = response.xpath("//li[contains(.,'Etage')]/span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("/")[0].strip())
        parking = response.xpath("//li[contains(.,'Garage') and contains(.,'m²')]//text() | //li[contains(.,'Parking') and contains(.,'m²')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//li[contains(.,'Parking')]/text()").get()
            if parking:
                item_loader.add_value("parking", True)
        furnished = response.xpath("//li[.='Meublé']/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        washing_machine = response.xpath("//li[.='Lave-linge']/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        dishwasher = response.xpath("//li[.='Lave-vaisselle']/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        swimming_pool = response.xpath("//h1//text()[contains(.,'PISCINE')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        terrace = response.xpath("//li[contains(.,'Terrasse') and contains(.,'m²')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        balcony = response.xpath("//li[contains(.,'Balcon') and contains(.,'m²')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        elevator = response.xpath("//li[.='Ascenseur']/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
     
        script_map = response.xpath("//script[contains(.,'L.marker([') and contains(.,'],{icon:icon_type_1')]/text()").get()
        if script_map:
            latlng = script_map.split("],{icon:icon_type_1")[0].split("L.marker([")[-1].strip()
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].split("]")[0].strip())
        else:
            script_map = response.xpath("//script[contains(.,'L.marker([')]/text()").get()
            if script_map:
                item_loader.add_value("latitude", script_map.split("L.marker([")[1].split(",")[0].strip())
                item_loader.add_value("longitude", script_map.split("L.marker([")[1].split(",")[1].split("]")[0].strip())
        description = " ".join(response.xpath("//p[@id='description']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        images = [x for x in response.xpath("//section[@class='showPictures']//a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
 
        rent = response.xpath("//section[@class='showPictures']//li[contains(.,'/ Mois')]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace("\xa0",".").replace(" ",""))
    
        item_loader.add_xpath("deposit", "//li[contains(.,'Dépôt de garantie')]/span/text()")
        item_loader.add_xpath("utilities", "//li[text()='Charges']/span/text()")

        item_loader.add_xpath("landlord_name", "//div[@class='userBlock']/p[@class='smallIcon userName']/strong/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='userBlock']//span[@class='phone smallIcon']/a/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='userBlock']//span[@class='mail smallIcon']/a/text()")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "local" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("studio" in p_type_string.lower() or "t1" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "t2" in p_type_string.lower() or "t3" in p_type_string.lower() or "t4" in p_type_string.lower() or "t5" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None