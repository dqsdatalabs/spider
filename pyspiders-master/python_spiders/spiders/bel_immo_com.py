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
    name = 'bel_immo_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = 'Bel_Immo_PySpider_france'
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
                "property_type" : "apartment",
                "type" : "2"
            },
        ]

        for item in start_urls:
            formdata = {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": item.get("type"),
                "data[Search][prixmax]": "",
                "data[Search][piecesmin]": "",
                "data[Search][NO_DOSSIER]": "",
                "data[Search][distance_idvillecode]": "",
                "data[Search][prixmin]": "",
                "data[Search][surfmin]": "",
            }
            yield FormRequest(
                "https://www.bel-immo.com/recherche/",
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":item["property_type"]
                }

            )
       


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='block-link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})

        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.xpath("//title/text()").get()
        if title:
            print(title)
            item_loader.add_value("title", title.strip())

        if "studio" in title.lower():
            item_loader.add_value("property_type","studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//span[@itemprop='productID']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split('Ref')[-1].strip())

        address = ""
        city = response.xpath("//span[contains(.,'Ville')]/following-sibling::span/text()").get()
        if city:
            address += city.strip() + " "
            item_loader.add_value("city", city.strip())

        zipcode = response.xpath("//span[contains(.,'Code postal')]/following-sibling::span/text()").get()
        if zipcode:
            address += "(" + zipcode.strip() + ")"
            item_loader.add_value("zipcode", zipcode.strip())

        if address:
            item_loader.add_value("address", address.strip())
        
        description = " ".join(response.xpath("//p[@itemprop='description']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//span[contains(.,'Surface habitable')]/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].split('.')[0].strip())

        room_count = response.xpath("//span[contains(.,'Nombre de chambre')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//span[contains(.,'Nb de salle de bains') or contains(.,\"d'eau\")]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("normalize-space(//span[contains(.,'Loyer')]/following-sibling::span/text())").get()
        if rent:
            rent = rent.split('€')[0].strip().replace(' ', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')
        
        deposit = response.xpath("normalize-space(//span[contains(.,'Dépôt de garantie')]/following-sibling::span/text())").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[0].replace(' ', '').strip())
        
        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude = response.xpath("//script[contains(.,'getMap')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('lat : ')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('lng: ')[1].split('}')[0].strip())
        
        floor = response.xpath("//span[contains(.,'Etage')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        utilities = response.xpath("//span[contains(.,'Dont état des lieux')]/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities", "".join(filter(str.isnumeric, utilities.strip())))
        
        parking = response.xpath("//span[contains(.,'Nombre de parking') or contains(.,'Nombre de garage')]/following-sibling::span/text()").get()
        if parking:
            if int(parking.strip()) > 0:
                item_loader.add_value("parking", True)
            elif int(parking.strip()) == 0:
                item_loader.add_value("parking", False)

        furnished = response.xpath("//span[contains(.,'Meublé')]/following-sibling::span/text()").get()
        if furnished:
            if furnished.strip().lower() == 'oui':
                item_loader.add_value("furnished", True)
            elif furnished.strip().lower() == 'non':
                item_loader.add_value("furnished", False)

        elevator = response.xpath("//span[contains(.,'Ascenseur')]/following-sibling::span/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                item_loader.add_value("elevator", True)
            elif elevator.strip().lower() == 'non':
                item_loader.add_value("elevator", False)

        terrace = response.xpath("//span[contains(.,'Terrasse')]/following-sibling::span/text()").get()
        if terrace:
            if terrace.strip().lower() == 'oui':
                item_loader.add_value("terrace", True)
            elif terrace.strip().lower() == 'non':
                item_loader.add_value("terrace", False)

        item_loader.add_value("landlord_name", "Bel Immo")
        item_loader.add_value("landlord_phone", "04 94 00 44 55")
        item_loader.add_value("landlord_email", "info@bel-immo.com")

        yield item_loader.load_item()

