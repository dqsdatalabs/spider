# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
import re

class MySpider(Spider):
    name = 'patrimandco_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Patrimandco_PySpider_france_fr'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.patrimandco.fr/biens?location_ou_vente=3&city=&postal_code=&field_type_de_bien_tid=1&field_type_de_taille_tid=All&field_superficie_value=&field_prix_value%5Bmin%5D=&field_prix_value%5Bmax%5D=", "property_type": "house"},
            {"url": "https://www.patrimandco.fr/biens?location_ou_vente=3&city=&postal_code=&field_type_de_bien_tid=2&field_type_de_taille_tid=All&field_superficie_value=&field_prix_value%5Bmin%5D=&field_prix_value%5Bmax%5D=", "property_type": "apartment"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='listing_biens']/li/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        next_page = response.xpath("//li[@class='pager-next']/a/@href").get()
        if next_page:
            p_url = response.urljoin(next_page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})  
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title.replace("Patrim and Co","").replace("Patrim and co",""))
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_value("external_source", "Patrimandco_PySpider_"+ self.country + "_" + self.locale)

        room_count = response.xpath("//ul[@class='list_propos']/li[contains(.,'pièce')]//text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1].strip())

        item_loader.add_xpath("bathroom_count", "substring-after(//ul[@class='list_propos']/li[contains(.,'Salle de bains')]/text(),': ')")
        
        rent_string = response.xpath("//div[@class='prix_bien']/text()").extract_first()
        if rent_string:
            item_loader.add_value("rent_string", rent_string.replace(" ",""))
       
        square = response.xpath("//ul[@class='list_propos']/li[contains(.,'Surface habitable')]//text()").extract_first()
        if square:
            square = square.split(":")[1].split("m")[0]
            square_meters = math.ceil(float(square.strip().replace(",",".")))
            item_loader.add_value("square_meters", str(square_meters)) 
         
        floor = response.xpath("//ul[@class='list_propos']/li[contains(.,'Etage')]//text()").extract_first()
        if floor:
            item_loader.add_value("floor",floor.split(":")[1].strip() )
     
        desc = "".join(response.xpath("//div[@id='description']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip().replace("Patrim and Co","").replace("Patrim and co",""))
        
        address = response.xpath("//div[@class='ville']//text()[normalize-space()]").extract_first()
        if address:
            for x in address.split(" "):
                if x.isdigit():
                    zipcode=x
                    item_loader.add_value("zipcode", zipcode.strip())
            if zipcode:
                item_loader.add_value("address", address.strip(str(zipcode)))
                item_loader.add_value("city", address.strip(str(zipcode)))
            else:
                item_loader.add_value("address", address)
                item_loader.add_value("city", address)

        energy_label =response.xpath("//div[@class='diagnostic_perf']//div[@class='valeur_chiffre']//text()").extract_first()
        if energy_label:
            item_loader.add_value("energy_label",energy_label_calculate(energy_label))

        latitude_longitude = response.xpath("//script[contains(.,'longitude')]//text()").get()
        if latitude_longitude:
                latitude = latitude_longitude.split('latitude":')[1].split(",")[0]
                longitude = latitude_longitude.split('"longitude":')[1].split("}")[0]
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
    
        parking = response.xpath("//ul[@class='list_propos']/li[contains(.,'Parking')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//ul[@class='list_propos']/li[contains(.,'Ascenseur ')]//text()").extract_first()
        if elevator:
            if "Non" in elevator:
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//ul[@class='list_propos']/li[contains(.,'Terrasse')]//text()").extract_first()
        if terrace:
                item_loader.add_value("terrace", True)

        balcony = response.xpath("//ul[@class='list_propos']/li[contains(.,'Balcon')]//text()").extract_first()
        if balcony:
                item_loader.add_value("balcony", True)
        
        images = [response.urljoin(x) for x in response.xpath("//ul[@class='slides']/li/img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "0561756833")
        item_loader.add_value("landlord_email", "contact@patrimandco.fr")
        item_loader.add_value("landlord_name", "Patrim & Co")
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