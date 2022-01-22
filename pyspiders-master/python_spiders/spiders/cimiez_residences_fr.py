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
    name = 'cimiez_residences_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        url = "https://www.blue-residences.fr/recherche/"
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Origin': 'https://www.blue-residences.fr',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Referer': 'https://www.blue-residences.fr/recherche/',
            'Accept-Language': 'tr,en;q=0.9',
        }
        start_urls = [
            {
                "formdata" : {
                    'data[Search][offredem]': '2',
                    'data[Search][idtype][]': '2'
                    },
                "property_type" : "apartment",
            },
            {
                "formdata" : {
                    'data[Search][offredem]': '2',
                    'data[Search][idtype][]': '25'
                    },
                "property_type" : "house",
            },
        ]
        for item in start_urls:
            yield FormRequest(url, formdata=item["formdata"], headers=headers, dont_filter=True, callback=self.parse, meta={'property_type': item["property_type"]})

    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@class='properties-v2__wrapper']/article/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 2 or seen:
            headers = {
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Referer': f'https://www.blue-residences.fr/recherche/{page}',
                'Accept-Language': 'tr,en;q=0.9',
            }
            yield Request(f"https://www.blue-residences.fr/recherche/{page}", 
                            callback=self.parse, 
                            headers=headers, 
                            dont_filter=True,
                            meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Cimiez_Residences_PySpider_france") 
        item_loader.add_xpath("title", "//title/text()") 
        item_loader.add_xpath("external_id", "//span[@class='detail-1__reference-number']/text()") 
        
        item_loader.add_xpath("city", "//div[span[.='Ville']]/span[2]/text()") 
        item_loader.add_xpath("zipcode", "//div[span[.='Code postal']]/span[2]/text()") 
        item_loader.add_value("address", "{} {}".format("".join(item_loader.get_collected_values("zipcode")),"".join(item_loader.get_collected_values("city"))))
        item_loader.add_xpath("floor", "//div[span[.='Etage']]/span[2]/text()") 

        item_loader.add_xpath("room_count", "//div[span[.='Nombre de chambre(s)']]/span[2]/text() | //div[span[.='Nombre de pièces']]/span[2]/text()") 
        item_loader.add_xpath("bathroom_count", "//div[@class='caracteristiques-pieces']/ul/li[strong[contains(.,'Nb. de salle de bain')]]/text()") 
        item_loader.add_xpath("latitude", "//div[@class='detail-1__map']//@data-lat") 
        item_loader.add_xpath("longitude", "//div[@class='detail-1__map']//@data-lng") 

        description = " ".join(response.xpath("//div[@class='detail-1__text']/p/text()").getall())  
        if description:
            item_loader.add_value("description", description.strip())

        rent = "".join(response.xpath("//div[@class='main-info__price']/span[1]/text()").extract())
        if rent:
            price = rent.replace("\xa0","").replace(" ","").strip()
            item_loader.add_value("rent_string", price.strip())

        meters = " ".join(response.xpath("//div[span[.='Surface habitable (m²)']]/span[2]/text()").getall())  
        if meters:
            s_meters = meters.split("m²")[0].replace(",",".").replace("\xa0","").replace(" ","").strip()
            item_loader.add_value("square_meters", int(float(s_meters))) 

        utilities = "".join(response.xpath("//div[span[contains(.,'Charges')]]/span[2]/text()[.!='Non renseigné']").extract())
        if utilities:
            uti = utilities.strip().replace("\xa0","")
            item_loader.add_value("utilities", uti.strip())

        deposit = "".join(response.xpath("//div[span[contains(.,'Dépôt de')]]/span[2]/text()[.!='Non renseigné']").extract())
        if deposit:
            deposit = deposit.strip().replace("\xa0","").replace(" ","")
            item_loader.add_value("deposit", deposit.strip())

        images = [x for x in response.xpath("//div[@class='swiper-slide slider-img__swiper-slide']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        elevator = "".join(response.xpath("//div[span[.='Ascenseur']]/span[2]/text()").getall())
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value("elevator",True)
            elif "non" in elevator.lower():
                item_loader.add_value("elevator",False)

        balcony = "".join(response.xpath("//div[span[.='Balcon']]/span[2]/text()").getall())
        if balcony:
            if "oui" in balcony.lower():
                item_loader.add_value("balcony",True)
            elif "non" in balcony.lower():
                item_loader.add_value("balcony",False)

        terrace = "".join(response.xpath("//div[span[.='Terrasse']]/span[2]/text()").getall())
        if terrace:
            if "oui" in terrace.lower():
                item_loader.add_value("terrace",True)
            elif "non" in terrace.lower():
                item_loader.add_value("terrace",False)

        parking = "".join(response.xpath("//div[span[.='Parking']]/span[2]/text()").getall())
        if parking:
            if parking !="0" or "oui" in parking.lower():
                item_loader.add_value("parking",True)
            elif parking == "0" or "non" in parking.lower():
                item_loader.add_value("parking",False)


        furnished = "".join(response.xpath("//div[span[.='Meublé']]/span[2]/text()").getall())
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished",True)
            elif "non" in furnished.lower():
                item_loader.add_value("furnished",False)

        item_loader.add_value("landlord_phone", "09 54 04 98 11")
        item_loader.add_value("landlord_name", "Cimiez residences")
        item_loader.add_value("landlord_email", "contact@cimiez-residences.fr")

        yield item_loader.load_item()