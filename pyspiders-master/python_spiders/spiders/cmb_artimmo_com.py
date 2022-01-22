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
    name = 'cmb_artimmo_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    post_url = "https://www.cmb-artimmo.com/recherche/"
    current_index = 0
    other_prop = ["4"]
    other_type = ["studio"]
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }

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
            p_url = f"https://www.cmb-artimmo.com/recherche/{page}"
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
        item_loader.add_value("external_source", "Cmb_Artimmo_PySpider_france")
        item_loader.add_value("external_id", response.url.split("/")[-1].split("-")[0])     
        
        title = " ".join(response.xpath("//h1[contains(@itemprop,'name')]//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//span[contains(.,'Ville')]//following-sibling::span//text()").get()
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//span[contains(.,'Ville')]//following-sibling::span//text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        
        zipcode = response.xpath("//span[contains(.,'Code')]//following-sibling::span//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        square_meters = response.xpath("//span[contains(.,'Surface habitable')]//following-sibling::span//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split("m")[0].split(",")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//span[contains(.,'Loyer')]//following-sibling::span//text()").get()
        if rent:
            rent = rent.strip().replace("€","").replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//span[contains(.,'Dépôt de garantie')]//following-sibling::span//text()").get()
        if deposit:
            deposit = deposit.strip().replace("€","").replace(" ","")
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//span[contains(.,'Charge')]//following-sibling::span//text()").get()
        if utilities:
            utilities = utilities.strip().replace("€","").replace(" ","")
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//p[contains(@itemprop,'desc')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[contains(.,'chambre')]//following-sibling::span//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//span[contains(.,'pièce')]//following-sibling::span//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//span[contains(.,'salle')]//following-sibling::span//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//ul[contains(@class,'imageGallery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        balcony = response.xpath("//span[contains(.,'Balcon')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//span[contains(.,'Meublé')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//span[contains(.,'Ascenseur')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//span[contains(.,'Etage')]//following-sibling::span//text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        latitude_longitude = response.xpath("//script[contains(.,'setCenter')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('map, { ')[1].split("lat: ")[1].split(',')[0]
            longitude = latitude_longitude.split('map, { ')[1].split('lng: ')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "CMB ARTİMMO")
        item_loader.add_value("landlord_phone", "01 58 05 36 12")
        item_loader.add_value("landlord_email", "contact@cmb-artimmo.com")

        yield item_loader.load_item()