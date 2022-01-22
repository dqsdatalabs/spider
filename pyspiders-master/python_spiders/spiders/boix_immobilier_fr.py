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
    name = 'boix_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "https://www.boix-immobilier.fr/recherche/"
    current_index = 0
    other_prop = ["4"]
    other_type = ["studio"]
    external_source="Boix_Immobilier_PySpider_france"
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
        yield FormRequest(self.post_url,
                        callback=self.parse,
                        formdata=formdata,
                        dont_filter=True,
                        meta={'property_type': "apartment"})

            
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for url in response.xpath("//ul[@class='listingUL']/li/@onclick").getall():
            follow_url = url.split("href='")[1].split("'")[0]
            yield Request(response.urljoin(follow_url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:            
            p_url = f"https://www.boix-immobilier.fr/recherche/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
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
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("normalize-space(//h2/span[@itemprop='productID']/parent::h2/text())").get()
        item_loader.add_value("title", title)
        
        rent = response.xpath("//p[@class='data'][contains(.,'Prix')]/span/text()[not(contains(.,'Nous'))]").get()
        if rent:
            rent = rent.split("€")[0].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//p[@class='data'][contains(.,'garantie')]/span/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip()
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//p[@class='data'][contains(.,'Charge')]/span/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        address = response.xpath("//p[@class='data'][contains(.,'Ville')]/span/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())
        
        zipcode = response.xpath("//p[@class='data'][contains(.,'Code postal')]/span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
            
        square_meters = response.xpath("//p[@class='data'][contains(.,'habitable')]/span/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//p[@class='data'][contains(.,'chambre')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//p[@class='data'][contains(.,'pièce')]/span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//p[@class='data'][contains(.,'salle')]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        furnished = response.xpath("//p[@class='data'][contains(.,'Meublé')]/span/text()").get()
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//p[@class='data'][contains(.,'Ascenseur')]/span/text()").get()
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
        
        parking = response.xpath("//p[@class='data'][contains(.,'parking')]/span/text()").get()
        if parking and "0" not in parking.lower():
            item_loader.add_value("parking", True)
        parkingcheck=item_loader.get_output_value("parking")
        if not parkingcheck:
            garage = response.xpath("//p[@class='data'][contains(.,'garage')]/span/text()").get()
            if garage:
                if "0" not in garage.lower():
                    item_loader.add_value("parking", True)
                
        
        terrace = response.xpath("//p[@class='data'][contains(.,'Terrasse')]/span/text()").get()
        if terrace:
            if "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
        
        balcony = response.xpath("//p[@class='data'][contains(.,'Balcon')]/span/text()").get()
        if balcony:
            if "oui" in balcony.lower():
                item_loader.add_value("balcony", True)
        
        floor = response.xpath("//p[@class='data'][contains(.,'Etage')]/span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        external_id = response.xpath("substring-after(//span[@itemprop='productID']/text(),':')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        description = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
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
        
        item_loader.add_value("landlord_name", "Boix Immobilier")
        item_loader.add_value("landlord_phone", "04 67 51 50 50")
        item_loader.add_value("landlord_email", "infos@boix-immobilier.fr")
        
        yield item_loader.load_item()