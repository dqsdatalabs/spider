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
    name = 'juron_tripier_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    def start_requests(self):
        yield Request(
            url="http://www.juron-tripier.fr/locations.html",
            callback=self.jump,
        )
    
    def jump(self, response):
        yield Request(
            url="http://www.juron-tripier.fr/components/locations/ajax.php?localisation=&latitude=&longitude=&perimetreRecherche=10&budgetMin=&budgetMax=&surfaceMin=&surfaceMax=",
            callback=self.parse,
            headers={
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
                "Connection": "keep-alive",
                "Host": "www.juron-tripier.fr",
                "Referer": "http://www.juron-tripier.fr/locations.html",
                "X-Requested-With": "XMLHttpRequest",
            },
        )

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='btn']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = "".join(response.xpath("//h3//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = "".join(response.xpath("//b[contains(.,'comporte')]/following-sibling::text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return
        
        item_loader.add_value("external_source", "Juron_Tripier_PySpider_france")

        external_id = response.xpath("//b[contains(.,'RÉF')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h3//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = "".join(response.xpath("//h3//text()").getall())
        if address:
            if "RUE JULIETTE RECAMIER" in address:
                item_loader.add_value("address", "RUE JULIETTE RECAMIER")
                item_loader.add_value("city", "RUE JULIETTE RECAMIER")
                zipcode = address.lower().replace("a voir aussi","").split("-")[-1].strip()
                item_loader.add_value("zipcode", zipcode)
            else:
                address = address.lower().replace("a voir aussi","").replace("- p","").replace("3ème","").replace("rue a courbet","").split("-")[-1].strip()
                if "lyon" in address.lower():
                    zipcode = address.split(" ")
                    for i in zipcode:
                        if i.isdigit() and int(i) > 100:
                            item_loader.add_value("zipcode", i)
                            break
                    item_loader.add_value("address", "Lyon")
                    item_loader.add_value("city", "Lyon")
                else:
                    zipcode = address.split(" ")
                    for i in zipcode:
                        if i.isdigit() and int(i) > 100:
                            item_loader.add_value("zipcode", i)
                        else:
                            item_loader.add_value("address", i.capitalize())
                            item_loader.add_value("city", i.capitalize())

        square_meters = response.xpath("//b[contains(.,'Surface')]/text()").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//b[contains(.,'Loyer')]//parent::p/text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//b[contains(.,'Dépot de garantie')]//parent::p/text()").get()
        if deposit:
            deposit = deposit.replace("mois","").strip()
            item_loader.add_value("deposit", int(deposit)*int(rent))

        utilities = response.xpath("//b[contains(.,'Charge ')]//parent::p/text()").get()
        if utilities:
            utilities = utilities.replace("€","").strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//b[contains(.,'Ce bien comporte')]//parent::p//text() | //b[contains(.,'Ce bien comporte')]//parent::p//following-sibling::p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        energy_label = response.xpath("//p[contains(.,'DPE')]//text()[not(contains(.,'vierge') or contains(.,'EN COURS'))]").get()
        if energy_label:
            energy_label = energy_label.split(":")[1].strip()
            item_loader.add_value("energy_label", energy_label)
        
        images = [x for x in response.xpath("//div[contains(@class,'mySlides')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "REGIE JURON ET TRIPIER")
        item_loader.add_value("landlord_phone", "04 72 56 20 20")
        item_loader.add_value("landlord_email", "regie@juron-tripier.fr")

        if not item_loader.get_collected_values("zipcode"):
            zipcode = response.xpath("//title/text()").get()
            if zipcode: item_loader.add_value("zipcode", zipcode.split("(")[-1].split(")")[0].strip())
        
        yield item_loader.load_item()
            

def get_p_type_string(p_type_string):
    if p_type_string and "parking" in p_type_string.lower():
        return None
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "f1" in p_type_string.lower() or "f2" in p_type_string.lower() or "f6" in p_type_string.lower() or "f5" in p_type_string.lower() or "f7" in p_type_string.lower() or "f3" in p_type_string.lower() or "f4" in p_type_string.lower() or "flat" in p_type_string.lower() or "appartement" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "maison" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None