# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'carletta_fr'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="Carletta_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.carletta.fr/fr/locations-biens-immobiliers.htm?_typebase=2&_typebien%5B%5D=1&prixloyerchargecomprise%5B%5D=&prixloyerchargecomprise%5B%5D=&_motsclefs="
                ],
                "property_type": "apartment"
            },
	     
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        for url in response.xpath("//div[@class='one-page']//a[contains(.,'Détail')]/@href").getall():       
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
  
        next_page = response.xpath("//div[@class='rampe-droit']//a[.='>']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//div[contains(@id,'reference')]//text()").get()
        if external_id:
            external_id = external_id.split("Réf.")[1].replace("\r","").replace("\n","")
            item_loader.add_value("external_id", external_id.strip())

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@id,'lieu-detail')]//text()").get()
        if address:
            city = address.split("-")[0].strip()
            zipcode = address.split("-")[-1].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        square_meters = response.xpath("//div[contains(@class,'champsSPEC-element')][contains(.,'Surface habitable')]//following-sibling::div//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split("m")[0].split(".")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//div[contains(@class,'champsSPEC-element')][contains(.,'Prix')]//following-sibling::div//text()").get()
        if rent:
            rent = rent.strip().split("€")[0].replace(" ","")
            item_loader.add_value("rent", rent)
        else:
            rent2 = response.xpath("//div[@id='prix-immobilier-detail']/text()").get().replace(" ","")
            item_loader.add_value("rent",rent2)
            


            
        item_loader.add_value("currency", "EUR")

        deposit = "".join(response.xpath("//span[contains(@class,'financierStd')][contains(.,'Dépôt de garantie')]//text()").getall())
        if deposit:
            deposit = deposit.split("garantie  :")[1].split("€")[0]
            item_loader.add_value("deposit", deposit)

        utilities = "".join(response.xpath("//div[contains(@class,'champsSPEC-element')][contains(.,'Provisions charges')]//following-sibling::div//text()").getall())
        if utilities:
            utilities = utilities.split("€")[0].strip()
            if utilities != "0":
                item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[contains(@id,'texte-detail')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'champsSPEC-element')][contains(.,'pièces')]//following-sibling::div//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div[contains(@class,'champsSPEC-element')][contains(.,'pièce')]//following-sibling::div//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//div[contains(@class,'champsSPEC-element')][contains(.,'salle')]//following-sibling::div//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x.split("url(")[1].split(")")[0] for x in response.xpath("//div[contains(@class,'photo')]//div[contains(@style,'background')]//@style").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//div[contains(@class,'champsSPEC-element')][contains(.,'parking') or contains(.,'garage')]//following-sibling::div//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(@class,'champsSPEC-element')][contains(.,'balcon')]//following-sibling::div//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[contains(@class,'champsSPEC-element')][contains(.,'terrasse')]//following-sibling::div//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//div[contains(@class,'champsSPEC-element')][contains(.,'Meublé')]//following-sibling::div//text()[contains(.,'Oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[contains(@class,'champsSPEC-element')][contains(.,'Ascenseur')]//following-sibling::div//text()[contains(.,'Oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//div[contains(@class,'champsSPEC-element')][contains(.,'Etage')]//following-sibling::div//text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        energy_label = response.xpath("//div[contains(@id,'valeur-dpe')]//text()").get()
        if energy_label:
            energy_label = energy_label.split(".")[0]
            item_loader.add_value("energy_label", find_energy_label(energy_label))
            
        swimming_pool = response.xpath("//div[contains(@class,'champsSPEC-element')][contains(.,'Piscine')]//following-sibling::div//text()[contains(.,'Oui')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        item_loader.add_value("landlord_name", "CARLETTA IMMOBILIER")
        item_loader.add_value("landlord_phone", "04 93 01 90 59")
        item_loader.add_value("landlord_email", "immobilier@carletta.fr")
   
        yield item_loader.load_item()


def find_energy_label(energy_number):
    energy_number = int(energy_number)
    if energy_number <= 50:
        return "A"
    elif energy_number <= 90:
        return "B"
    elif energy_number <= 150:
        return "C"
    elif energy_number <= 230:
        return "D"
    elif energy_number <= 330:
        return "E"
    elif energy_number <= 450:
        return "F"
    else:
        return "G"