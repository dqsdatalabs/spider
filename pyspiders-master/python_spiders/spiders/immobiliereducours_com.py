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
    name = 'immobiliereducours_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "http://www.immobiliereducours.com/recherche/"
    current_index = 0
    other_prop = ["1", "4", "18"]
    other_type = ["house", "studio", "apartment"]
    def start_requests(self):
        formdata = {
            "data[Search][offredem]": "2",
            "data[Search][idvillecode]": "void",
            "data[Search][idtype]": "2",
            "data[Search][pieces]": "void",
            "data[Search][surfmin]": "",
            "data[Search][surfmax]": "",
            "data[Search][NO_DOSSIER]": "",
            "data[Search][prixmin]": "",
            "data[Search][prixmax]": "",
        }
        yield FormRequest(self.post_url,
                        callback=self.parse,
                        formdata=formdata,
                        dont_filter=True,
                        meta={'property_type': "apartment"})

            
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        for url in response.xpath("//h1/a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        if response.xpath("//span[contains(.,'»')]/../@href").get():
            p_url = f"http://www.immobiliereducours.com/recherche/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "data[Search][offredem]": "2",
                "data[Search][idvillecode]": "void",
                "data[Search][idtype]": self.other_prop[self.current_index],
                "data[Search][pieces]": "void",
                "data[Search][surfmin]": "",
                "data[Search][surfmax]": "",
                "data[Search][NO_DOSSIER]": "",
                "data[Search][prixmin]": "",
                "data[Search][prixmax]": "",
            }
            yield FormRequest(self.post_url,
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': self.other_type[self.current_index],})
            self.current_index += 1

                
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Immobiliereducours_PySpider_france")
        
        title = response.xpath("normalize-space(//div[contains(@class,'bienTitle')]/h1/text())").get()
        item_loader.add_value("title", title)
        
        external_id = response.xpath("//span[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Ref")[1].strip())
        
        rent = response.xpath("//p[@class='data'][contains(.,'Loyer')]/span[2]/text()[not(contains(.,'Nous'))]").get()
        if rent:
            rent = rent.split("€")[0].replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//p[@class='data'][contains(.,'garantie')]/span[2]/text()[not(contains(.,'Non'))]").get()
        if deposit:
            deposit = deposit.split("€")[0].replace(" ","")
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//p[@class='data'][contains(.,'Charges')]/span[2]/text()[not(contains(.,'Non'))]").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        address = response.xpath("//p[@class='data'][contains(.,'Ville')]/span[2]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())
        
        zipcode = response.xpath("//p[@class='data'][contains(.,'Code')]/span[2]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
            
        square_meters = response.xpath("//p[@class='data'][contains(.,'habitable')]/span[2]/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//p[@class='data'][contains(.,'chambre')]/span[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//p[@class='data'][contains(.,'pièce')]/span[2]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//p[@class='data'][contains(.,'salle')]/span[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        furnished = response.xpath("//p[@class='data'][contains(.,'Meublé')]/span[2]/text()").get()
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//p[@class='data'][contains(.,'Ascenseur')]/span[2]/text()").get()
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
        
        parking = response.xpath("//p[@class='data'][contains(.,'parking')]/span[2]/text()").get()
        garage = response.xpath("//p[@class='data'][contains(.,'garage')]/span[2]/text()").get()
        if parking:
            if "0" not in parking.lower():
                item_loader.add_value("parking", True)
        if garage:
            if "0" not in garage.lower():
                item_loader.add_value("parking", True)
                
        terrace = response.xpath("//p[@class='data'][contains(.,'Terrasse')]/span[2]/text()").get()
        if terrace:
            if "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
        
        balcony = response.xpath("//p[@class='data'][contains(.,'Balcon')]/span[2]/text()").get()
        if balcony:
            if "oui" in balcony.lower():
                item_loader.add_value("balcony", True)
        
        floor = response.xpath("//p[@class='data'][contains(.,'Etage')]/span[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
                
        description = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        import dateparser
        if "disponible" in description.lower():
            available_date = description.lower().split("disponible")[1].replace(",","-").split("-")[0].strip()
            date_parsed = dateparser.parse(available_date.replace("début","").strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//ul[contains(@class,'imageGallery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            
        latitude_longitude = response.xpath("//script[contains(.,'lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat :')[1].split(',')[0]
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "Immobiliere Du Cours")
        item_loader.add_value("landlord_phone", "04 42 40 34 47")
        item_loader.add_value("landlord_email", "immobiliere-du-cours@orange.fr")
        
        yield item_loader.load_item()