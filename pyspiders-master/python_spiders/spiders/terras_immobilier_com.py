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

class MySpider(Spider):
    name = 'terras_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Terras_Immobilier_PySpider_france"
    # 1. FOLLOWING
    def start_requests(self):
        start_urls = [
            {
                "property_type" : "house",
                "type" : "1"
            },
        ]
        for item in start_urls:
            formdata = {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": item["type"],
                "data[Search][prixmax]": "",
                "data[Search][piecesmin]": "",
                "data[Search][surfmin]": "",
                "data[Search][distance_idvillecode]": "",
                "data[Search][prixmin]": "",
                "data[Search][NO_DOSSIER]": "",
            }
            yield FormRequest(
                "https://www.terras-immobilier.com/recherche/",
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":item["property_type"]
                }

            )
    

    def parse(self, response):
        for item in response.xpath("//button[contains(.,'Détails')]/@data-url").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={
                    "property_type":response.meta["property_type"]
                })
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        title = response.xpath("//h1[@class='titleBien']/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        address = response.xpath("//li[contains(.,'Ville')]/text()").get()
        if address:
            item_loader.add_value("address", address.split(":")[1].strip())
            item_loader.add_value("city", address.split(":")[1].strip())
        
        zipcode = response.xpath("//li[contains(.,'Code')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(":")[1].strip())
        
        rent = response.xpath("//li[contains(.,'Loyer')]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split(":")[1].split("€")[0].replace(" ",""))
            item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//li[contains(.,'habitable')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(":")[1].split("m")[0].strip())
        
        room_count = response.xpath("//li[contains(.,'chambre')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1].strip())
        
        bathroom_count = response.xpath("//li[contains(.,'salle')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[1].strip())
        
        garage = response.xpath("//li[contains(.,'garage')]/text()[.!='0']").get()
        if garage:
            item_loader.add_value("garage", True)
        
        terrace = response.xpath("//li[contains(.,'Terrasse')]/text()").get()
        if terrace:
            if "oui" in terrace.split(":")[1].lower():
                item_loader.add_value("terrace", True)
            elif "non" in terrace.split(":")[1].lower():
                item_loader.add_value("terrace", False)
        
        furnished = response.xpath("//li[contains(.,'Meublé')]/text()").get()
        if furnished:
            if "oui" in furnished.split(":")[1].lower():
                item_loader.add_value("furnished", True)
            elif "non" in furnished.split(":")[1].lower():
                item_loader.add_value("furnished", False)
        
        external_id = response.xpath("//p[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        deposit = response.xpath("//li[contains(.,'garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].split("€")[0].replace(" ",""))
        
        utilities = response.xpath("//li[contains(.,'Honoraires')]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].split("€")[0].replace(" ",""))

        desc = " ".join(response.xpath("//div[@class='offreContent']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//ul[@class='slider_Mdl']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        swimming_pool = response.xpath("//li[contains(.,'Terrain piscinable')]/text()").get()
        if swimming_pool:
            if "oui" in swimming_pool.split(":")[1].lower():
                item_loader.add_value("swimming_pool", True)
            elif "non" in swimming_pool.split(":")[1].lower():
                item_loader.add_value("swimming_pool", False)
        
        latitude_longitude = response.xpath("//script[contains(.,'lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat :")[1].split(",")[0].strip()
            longitude = latitude_longitude.split("lng:")[1].split(",")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "TERRAS IMMOBILIER")
        item_loader.add_value("landlord_phone", "04 90 37 38 19")
        item_loader.add_value("landlord_email", "contact@terras-immobilier.com")
        
        
        yield item_loader.load_item()
