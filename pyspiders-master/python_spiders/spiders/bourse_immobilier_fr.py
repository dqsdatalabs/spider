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

class MySpider(Spider): 
    name = 'bourse_immobilier_fr'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="Bourseimmobilier_PySpider_france_fr"
    #https://www.bourse-immobilier.fr/louer/annonces-immobilieres-location
    start_urls = ["https://www.human-immobilier.fr/louer/annonces-immobilieres-location"] #LEVEL-1

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)

        seen = False

        for item in response.xpath("//div[@class='top top-bien-location']/a/@href").getall():
            f_url = response.urljoin(item)
            prop_type = response.xpath("//span[@class='typeBien']/text()").get()
            if prop_type:
                 
                if "appartement" in prop_type.lower():
                    prop_type = "apartment"
                elif "Maison" in prop_type:
                    prop_type = "house"
                else:
                    prop_type = None
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : prop_type},
            )
            seen = True
        if page == 2 or seen:
            url = f"https://www.human-immobilier.fr/louer/annonces-immobilieres-location?page={page}"
            yield Request(url=url,
                                callback=self.parse,
                                meta={"page": page+1})
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status= response.url
        if "parking" not in status.lower():
            item_loader.add_value("external_source", self.external_source)

            prop_type = response.meta.get('property_type')
            if prop_type:
                item_loader.add_value("property_type", prop_type)
            property_check=item_loader.get_output_value("property_type")
            if not property_check:
                property=response.xpath("//span[@class='title']/text()").get()
                if property and "appartement".lower():
                    item_loader.add_value("property_type","apartment")
            error=response.url
            if error and "Erreur" in error:
                return
                
            item_loader.add_value("external_link", response.url)
            title=response.xpath("//title/text()").get()
            if title:
                item_loader.add_value("title", title)
            
            rent=response.xpath("//span[@class='price-format']/text()").get()
            if rent:
                rent=rent.replace("\n","").replace("\r","").split("€")[0].replace(" ","").strip()
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency","EUR")

            square_meters=response.xpath("//span[contains(.,'Surface')]/following-sibling::span/text()").get()
            if square_meters:
                item_loader.add_value("square_meters", square_meters.split('m²')[0].strip())
            
            room_count=response.xpath("//span[contains(.,'Nombre de pièces')]/following-sibling::span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
            bathroom_count=response.xpath("//span[contains(.,'Salle de bains')]/following-sibling::span/text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)

            city=response.xpath("//span[@class='ville']/text()").get()
            if city:
                city=city.split("|")[0].strip()
                item_loader.add_value("city",city)
            zipcode=response.xpath("//title/text()").get()
            if zipcode:
                zipcode=zipcode.split("(")[-1].split(")")[0]
                item_loader.add_value("zipcode",zipcode)
            address=item_loader.get_output_value("city")+" "+item_loader.get_output_value("zipcode")
            if address:
                item_loader.add_value("address",address)

            
            external_id=response.url
            if external_id:
                item_loader.add_value("external_id", external_id.split('_')[-1].strip())

            desc="".join(response.xpath("//div[@class='descriptif']/p/text()").getall())
            if desc:
                item_loader.add_value("description", re.sub("\s{2,}", " ", desc))
                
            # images=[x for x in response.xpath("//a[@class='spanPhoto']//img[@class='img-fluid']/@src").getall()]
            # if images:
            #     item_loader.add_value("images", images)
            #     item_loader.add_value("external_images_count", str(len(images)))
            
            item_loader.add_value("landlord_name","Human Immobilier")
            item_loader.add_xpath("landlord_email","//input[@name='EmaiLDestinataire']/@value")
            item_loader.add_xpath("landlord_phone","//div[@class='modal-header']//span[@id='telephone']/text()")
            
            furnished=response.xpath("//span[contains(.,'Meublé')]/following-sibling::span/text()").get()
            if furnished:
                if "non" in furnished.lower():
                    item_loader.add_value("furnished", False)
                else:
                    item_loader.add_value("furnished", True)
            
            deposit=response.xpath("//p[contains(.,'Depôt de garantie')]/span/text()").get()
            if deposit:
                item_loader.add_value("deposit", deposit.strip().replace(" ","").split("€")[0])
            # utilities = response.xpath("substring-after(//div[@id='detailprix']//text()[contains(.,'charges')],'dont')").get()
            # if utilities:
            #     try:
            #         utilities = utilities.split("€")[0].strip()
            #         if utilities.isdigit():
            #             item_loader.add_value("utilities", utilities)   
            #     except:
            #         pass 
            # parking=response.xpath("//ul/li[contains(.,'Parking')]/span[2]/text()").get()
            # garage=response.xpath("//ul/li[contains(.,'Garage')]/span[2]/text()").get()
            # if parking or garage:
            #     item_loader.add_value("parking",True)
                
            energy_label=response.xpath("//span[.='Diagnostic de Performance Energétique']//following-sibling::div[@class='DPE_cursor']/div//span//text()").get()
            if energy_label:
                energy = energy_label.replace("(","").replace(")","")
                item_loader.add_value("energy_label",energy_label_calculate(int(float(energy.replace(",",".")))))

            latitude = response.xpath("//input[@id='Lat']//@value").get()
            if latitude:  
                item_loader.add_value("latitude", latitude)
            longitude = response.xpath("//input[@id='Long']//@value").get()
            if longitude:  
                item_loader.add_value("longitude", latitude)

            
            images = [x for x in response.xpath("//div[@class='photo']//a[@class='spanPhoto']//@data-lc-href").getall()]
            if images:
                item_loader.add_value("images", images)    


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