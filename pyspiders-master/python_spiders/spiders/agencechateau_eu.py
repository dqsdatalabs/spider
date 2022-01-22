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
    name = 'agencechateau_eu'
    execution_type='testing'
    country='france'
    locale='fr'
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
        start_urls = [
            {
                "type" : "2",
                "property_type" : "apartment"
            },
            {
                "type" : "4",
                "property_type" : "studio"
            },
        ] #LEVEL-1
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Origin': 'http://www.agencechateau.eu',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Referer': 'http://www.agencechateau.eu/recherche/',
            'Accept-Language': 'tr,en;q=0.9',
        }

        for url in start_urls:
            r_type = str(url.get("type"))
            payload = {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": r_type, 
                "data[Search][prixmax]": "",
                "data[Search][piecesmin]": "",
                "data[Search][NO_DOSSIER]": "",
                "data[Search][distance_idvillecode]": "",
                "data[Search][prixmin]": "",
                "data[Search][piecesmax]": "",
                "data[Search][surfmin]": "",
            }

            yield FormRequest(url="http://www.agencechateau.eu/recherche/",
                                callback=self.parse,
                                dont_filter=True,
                                headers=headers,
                                formdata=payload,
                                meta={'property_type': url.get('property_type')})
            
    # 1. FOLLOWING
    def parse(self, response): 

        for item in response.xpath("//ul[@class='listingUL']//a[contains(.,'voir')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type" : response.meta["property_type"]})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Agencechateau_PySpider_france")

        external_id = response.xpath("//span[contains(@itemprop,'productID')]//text()").get()
        if external_id:
            external_id = external_id.split("Ref")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = "".join(response.xpath("//div[contains(@class,'bienTitle')]//h2//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        zipcode = response.xpath("normalize-space(//span[contains(.,'Code postal')]//following-sibling::span/text())").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        address = response.xpath("normalize-space(//span[contains(.,'Ville')]//following-sibling::span/text())").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)

        rent = response.xpath("normalize-space(//span[contains(.,'Loyer')]//following-sibling::span/text())").get()
        if rent:
            rent = rent.replace(" ","").replace("€","")
            item_loader.add_value("rent", rent)
        
        item_loader.add_value("currency", "EUR")
        deposit = response.xpath("normalize-space(//span[contains(.,'de garantie')]//following-sibling::span/text())").get()
        if deposit:
            deposit = deposit.replace(" ","").replace("€","")
            if deposit.isdigit():
                item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("normalize-space(//span[contains(.,'Charge')]//following-sibling::span/text())").get()
        if utilities:
            utilities = utilities.replace(",",".").replace("€","")
            item_loader.add_value("utilities", int(float(utilities)))

        square_meters = response.xpath("normalize-space(//span[contains(.,'Surface')]//following-sibling::span/text())").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("normalize-space(//span[contains(.,'chambre')]//following-sibling::span/text())").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("normalize-space(//span[contains(.,'pièces')]//following-sibling::span/text())").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
    
        bathroom_count = response.xpath("normalize-space(//span[contains(.,'salle')]//following-sibling::span/text())").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        floor = response.xpath("normalize-space(//span[contains(.,'Etage')]//following-sibling::span/text())").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        furnished = response.xpath("normalize-space(//span[contains(.,'Meublé')]//following-sibling::span/text()[contains(.,'OUI')])").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        elevator = response.xpath("normalize-space(//span[contains(.,'Ascenseur')]//following-sibling::span/text()[contains(.,'OUI')])").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("normalize-space(//span[contains(.,'Terrasse')]//following-sibling::span/text()[contains(.,'OUI')])").get()
        if terrace:
            item_loader.add_value("terrace", True)

        balcony = response.xpath("normalize-space(//span[contains(.,'Balcon')]//following-sibling::span/text()[contains(.,'OUI')])").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        desc = " ".join(response.xpath("//p[contains(@itemprop,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//ul[contains(@class,'imageGallery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'center:')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat :')[1].split(',')[0]
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Agence du château")
        item_loader.add_value("landlord_phone", "01 46 83 13 95")
        item_loader.add_value("landlord_email", "contact@agencechateau.fr")


        yield item_loader.load_item()