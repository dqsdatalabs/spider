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
    name = 'awimmo64_net'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    def start_requests(self):
        formdata = {
            "data[Search][offredem]": "2",
            "data[Search][idtype]": "2",
            "data[Search][surfmin]": "",
            "data[Search][surfmax]": "",
            "data[Search][piecesmin]": "",
            "data[Search][piecesmax]": "",
            "data[Search][prixmin]": "",
            "data[Search][prixmax]": "",
            "data[Search][NO_DOSSIER]": "",
            "data[Search][idvillecode]": "void",
            "data[Search][distance_idvillecode]": "",
        }
        yield FormRequest("https://www.awimmo64.net/recherche/",
                        callback=self.parse,
                        formdata=formdata,
                        dont_filter=True,
                        meta={'property_type': "apartment"})

            
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for url in response.xpath("//div[@class='bienTitle']//a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:            
            p_url = f"https://www.awimmo64.net/recherche/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
                
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Awimmo64_PySpider_france")
        item_loader.add_value("external_id", response.url.split("/")[-1].split("-")[0])     
        
        title = " ".join(response.xpath("//h2//text()").getall())
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

        deposit = response.xpath("//span[contains(.,'Dépôt de garantie')]//following-sibling::span//text()[not(contains(.,'Non'))]").get()
        if deposit:
            deposit = deposit.strip().replace("€","").replace(" ","")
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//span[contains(.,'Charge')]//following-sibling::span//text()[not(contains(.,'Non'))]").get()
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

        item_loader.add_value("landlord_name", "AWIMMO 64")
        item_loader.add_value("landlord_phone", "05 59 08 68 44")
        item_loader.add_value("landlord_email", "agence@awimmo64.com")

        yield item_loader.load_item()