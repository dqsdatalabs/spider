# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'dives_sur_mer_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Dives_Sur_Mer_Immobilier_PySpider_france'
    formdata_list = [
        {
            "property_type": "apartment",
            "formdata": {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": "2",
                "data[Search][idvillecode]": "void",
            },
        },
    ]

    def start_requests(self):

        yield FormRequest("http://www.dives-sur-mer-immobilier.fr/recherche/",
                        callback=self.parse,
                        formdata=self.formdata_list[0]["formdata"],
                        dont_filter=True,
                        meta={"property_type": self.formdata_list[0]["property_type"], "next_index": 1})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        next_index = response.meta.get("next_index", 1)
        seen = False

        for item in response.xpath("//span[contains(text(),'Voir le bien')]/../@onclick").getall():
            seen = True
            yield Request(response.urljoin(item.split("href='")[1].split("'")[0]), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 2 or seen:
            headers = {
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Referer': f"http://www.dives-sur-mer-immobilier.fr/recherche/{page}",
                'Accept-Language': 'tr,en;q=0.9',
            }   
            follow_url = f"http://www.dives-sur-mer-immobilier.fr/recherche/{page}"
            yield Request(follow_url, 
                        headers=headers, 
                        dont_filter=True, 
                        callback=self.parse, 
                        meta={"page": page + 1, "property_type": response.meta["property_type"], "next_index": next_index})
                      
        elif len(self.formdata_list) > next_index:
            yield FormRequest("http://www.dives-sur-mer-immobilier.fr/recherche/",
                            callback=self.parse,
                            formdata=self.formdata_list[next_index]["formdata"],
                            dont_filter=True,
                            meta={"property_type": self.formdata_list[next_index]["property_type"], "page": 2, "next_index": next_index + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//h1/span/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        external_id = response.xpath("//span[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(" ")[1])
            
        zipcode = response.xpath("//p/span[contains(.,'Code')]/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        city = response.xpath("//p/span[contains(.,'Ville')]/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("address", city.strip())
            item_loader.add_value("city", city.strip())
        
        square_meters = response.xpath("//p/span[contains(.,'habitable')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//p/span[contains(.,'chambre')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//p/span[contains(.,'pièce')]/following-sibling::span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//p/span[contains(.,'salle')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = response.xpath("//p/span[contains(.,'Loyer')]/following-sibling::span/text()").get()
        if rent:
            rent = rent.split("€")[0].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//p/span[contains(.,'Dépôt')]/following-sibling::span/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip()
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//p/span[contains(.,'Charge')]/following-sibling::span/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        description = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat :')[1].split(',')[0]
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        furnished = response.xpath("//p/span[contains(.,'Meublé')]/following-sibling::span/text()").get()
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        
        elevator = response.xpath("//p/span[contains(.,'Ascenseur')]/following-sibling::span/text()").get()
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
                
        item_loader.add_value("landlord_name", "DIVES SUR MER IMMOBILIER")
        item_loader.add_value("landlord_phone", "02 31 28 00 10")
        item_loader.add_value("landlord_email", "contact@dives-sur-mer-immobilier.fr")
        
        yield item_loader.load_item()