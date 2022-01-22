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
    name = 'immo_ligeis_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.immo.ligeis.fr/recherche-location/?_sft_nc_cat_type_bien=appartement&_sfm__mettre_carre=0++165+&_sfm__prix_loyer=0++5050+&sf_paged=1", 
                "property_type": "apartment"
            },
	        {
                "url": "https://www.immo.ligeis.fr/recherche-location/?_sft_nc_cat_type_bien=maison&_sfm__mettre_carre=0++165+&_sfm__prix_loyer=0++5050+&sf_paged=1", 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        for item in response.xpath("//article[contains(@id,'post')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type':response.meta.get('property_type')})
            seen=True
        
        if page ==2 or seen:      
            f_url = response.url.replace(f"paged={page-1}", f"paged={page}")
            yield Request(f_url, callback=self.parse, meta={"page": page+1, 'property_type':response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Immo_Ligeis_PySpider_france")
        prp_type = response.xpath("//div[contains(@class,'element')]/div[contains(.,'Catégorie')]/div[2]/text()[contains(.,'commercial')]").get()
        if prp_type:
            return
        external_id = response.xpath("//div[contains(@class,'element')]/div[contains(.,'Référence')]/div[2]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        title = " ".join(response.xpath("//div[@class='taille_site']//div[@class='col span_8 ']//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ',title))
        address = ", ".join(response.xpath("//div[contains(@class,'element')]/div[contains(.,'Quartier') or contains(.,'Ville')]/div[2]/text()").getall())
        if address:
            item_loader.add_value("address", re.sub('\s{2,}', ' ',address))

        city = response.xpath("//div[contains(@class,'element')]/div[contains(.,'Ville')]/div[2]/text()").get()
        if city:
            item_loader.add_value("city", city.strip())

        rent = response.xpath("//div[contains(@class,'element')]/div[contains(.,'Loyer mensuel')]/div[2]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split(".")[0])
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//div[contains(@class,'element')]/div[contains(.,'Dépôt de garantie')]/div[2]/text()").get()
        if deposit:
            deposit = deposit.split(".")[0].replace(",","").strip()
            item_loader.add_value("deposit", deposit)
        utilities = response.xpath("//div[contains(@class,'element')]/div[contains(.,'Charges')]/div[2]/text()").get()
        if utilities:
            utilities = utilities.split(".")[0].replace(",","").strip()
            item_loader.add_value("utilities", utilities)
        desc = " ".join(response.xpath("//div[@class='texte_bien']//p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        room_count = response.xpath("//div[contains(@class,'element')]/div[contains(.,'Nombre de chambre')]/div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//div[contains(@class,'element')]/div[contains(.,'Nombre de pièce')]/div[2]/text()")

        square_meters = response.xpath("//div[contains(@class,'element')]/div[contains(.,'Surface habitable')]/div[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", str(int(float(square_meters.split("m")[0].strip()))))
        floor = response.xpath("//div[contains(@class,'element')]/div[contains(.,'Etage')]/div[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
   
        item_loader.add_xpath("bathroom_count", "//div[contains(@class,'element')]/div[contains(.,'Salle d')]/div[2]/text()")
        energy_label = response.xpath("//div[contains(@class,'bilan_energie')]//div[@class='picto']//div[@class='picto']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        images = [x.split("('")[1].split("')")[0] for x in response.xpath("//div[@class='slides']/div[@class='slide_second']/div[@class='image_petite']/@style").getall()]
        if images:
            item_loader.add_value("images", images)        
        
        available_date = response.xpath("//div[contains(@class,'element')]/div[contains(.,'Disponibilité')]/div[2]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"], languages=['fr'])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        balcony = response.xpath("//div[contains(@class,'element')]/div[contains(.,'Balcon')]/div[2]/text()").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)
        elevator = response.xpath("//div[contains(@class,'element')]/div[contains(.,'Ascenseur')]/div[2]/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)

        latitude_longitude = response.xpath("//script[contains(.,' google.maps.LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(' google.maps.LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split(' google.maps.LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "LIGEIS IMMOBILIER")
        item_loader.add_value("landlord_phone", "02 41 880 880")

        yield item_loader.load_item()