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
import dateparser
from python_spiders.helper import ItemClear
class MySpider(Spider):
    name = 'contactimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "https://www.agence-contact-immo.fr/votre-recherche/"
    current_index = 0
    other_prop = ["1"]
    other_prop_type = ["house"]
    def start_requests(self):
        formdata = {
            "data[Search][offredem]": "2",
            "data[Search][idtype][]": "2",
            "data[Search][prixmax]": "",
            "data[Search][piecesmin]": "",
            "data[Search][NO_DOSSIER]": "",
            "data[Search][distance_idvillecode]": "",
            "data[Search][prixmin]": "",
            "data[Search][surfmin]": "",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            dont_filter=True,
            formdata=formdata,
            meta={
                "property_type":"apartment",
            }
        )


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='block-link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        if page == 2 or seen:
            p_url = f"https://www.agence-contact-immo.fr/votre-recherche/{page}"
            yield Request(p_url, dont_filter=True, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": self.other_prop[self.current_index],
                "data[Search][prixmax]": "",
                "data[Search][piecesmin]": "",
                "data[Search][NO_DOSSIER]": "",
                "data[Search][distance_idvillecode]": "",
                "data[Search][prixmin]": "",
                "data[Search][surfmin]": "",
            }
            yield FormRequest(
                url=self.post_url,
                callback=self.parse,
                dont_filter=True,
                formdata=formdata,
                meta={
                    "property_type":self.other_prop_type[self.current_index],
                }
            )
            self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])  
        item_loader.add_value("external_source", "Contactimmo_PySpider_france")
        
        title = response.xpath("normalize-space(//h2/text())").get()
        item_loader.add_value("title", title)
        
        external_id = response.xpath("//span[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(" ")[1])
        
        address = response.xpath("//p[contains(.,'Ville')]/span[2]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())
            
        zipcode = response.xpath("//p[contains(.,'Code')]/span[2]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        square_meters = response.xpath("//p[contains(.,'habitable')]/span[2]/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
            
        room_count = response.xpath("//p[contains(.,'chambre')]/span[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//p[contains(.,'salle')]/span[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = response.xpath("//p[contains(.,'Loyer')]/span[2]/text()").get()
        if rent:
            price = rent.split("€")[0].replace(" ","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        floor = response.xpath("//p[contains(.,'Etage')]/span[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        deposit = response.xpath("//p[contains(.,'Dépôt')]/span[2]/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].replace(" ","")
            item_loader.add_value("deposit", deposit)
               
        utilities = response.xpath("//p[contains(.,'Charges')]/span[2]/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].replace(" ","")
            item_loader.add_value("utilities", utilities)
        
        elevator = response.xpath("//p[contains(.,'Ascenseur')]/span[2]/text()[contains(.,'OUI') or contains(.,'Oui') or contains(.,'oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//p[contains(.,'Terrasse')]/span[2]/text()[contains(.,'OUI') or contains(.,'Oui') or contains(.,'oui')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        furnished = response.xpath("//p[contains(.,'Meublé')]/span[2]/text()[contains(.,'OUI') or contains(.,'Oui') or contains(.,'oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        balcony = response.xpath("//p[contains(.,'Balcon')]/span[2]/text()[contains(.,'OUI') or contains(.,'Oui') or contains(.,'oui')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//p[contains(.,'garage')]/span[2]/text()[not(contains(.,'0'))]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        description = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        if "Libre le" in description:
            available_date = description.split("Libre le")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat :')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "Contact-Immo Agence")
        item_loader.add_value("landlord_phone", "09 75 96 39 61")
        item_loader.add_value("landlord_email", "info@agence-contact-immo.fr")
        
        yield item_loader.load_item()