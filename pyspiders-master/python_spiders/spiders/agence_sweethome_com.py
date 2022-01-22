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
    name = 'agence_sweethome_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Agence_Sweethome_PySpider_france'
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
                "type" : 1,
                "property_type" : "house"
            },
            {
                "type" : 2,
                "property_type" : "apartment"
            },
            {
                "type" : 25,
                "property_type" : "house"
            },
            
        ] #LEVEL-1

        for url in start_urls:
            r_type = str(url.get("type"))
            payload = {
                "data[Search][offredem]": "2",
                "data[Search][idtype]": r_type,
                "data[Search][idvillecode]": "void",
            }

            yield FormRequest(url="https://www.agence-sweethome.com/recherche/",
                                callback=self.parse,
                                formdata=payload,
                                dont_filter=True,
                                meta={'property_type': url.get('property_type')})
            
    # 1. FOLLOWING
    def parse(self, response): 

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@class='property__content-wrapper']/a/@href").extract():
            seen = True
            yield Request(
                response.urljoin(item), 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        
        if page == 2 or seen:
            headers = {
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Referer': f'https://www.agence-sweethome.com/recherche/{page}',
                'Accept-Language': 'tr,en;q=0.9',
            }
            yield Request(
                f'https://www.agence-sweethome.com/recherche/{page}', 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type"), "page": page + 1})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)   
        if response.url == "https://www.agence-sweethome.com/recherche/2":
            return
        item_loader.add_xpath("title", "//div[@class='main-info__content-wrapper']//div[@class='title__content']/span/text()") 
        item_loader.add_xpath("external_id", "//div[@class='detail-1__reference']/span/text()")
        room_count = response.xpath("//div[span[.='Nombre de chambre(s)']]/span[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//div[span[.='Nombre de pièces']]/span[2]/text()")
        item_loader.add_xpath("floor", "//div[span[.='Etage']]/span[2]/text()")

        bathroom_count = response.xpath("//div[span[contains(.,'Nb de salle d')]]/span[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        address = response.xpath("concat(//div[span[.='Ville']]/span[2]/text(),', ',//div[span[.='Code postal']]/span[2]/text())").get()
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
        zipcode = response.xpath("//div[span[.='Code postal']]/span[2]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        city = response.xpath("//div[span[.='Ville']]/span[2]/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
       
        furnished = response.xpath("//div[span[.='Meublé']]/span[2]/text()").get()
        if furnished:
            if "NON" in furnished:
                item_loader.add_value("furnished", False)
            elif "OUI" in furnished:
                item_loader.add_value("furnished", True)

        terrace = response.xpath("//div[span[.='Terrasse']]/span[2]/text()").get()
        if terrace:
            if "NON" in terrace:
                item_loader.add_value("terrace", False)
            elif "OUI" in terrace:
                item_loader.add_value("terrace", True)
        balcony = response.xpath("//div[span[.='Terrasse']]/span[2]/text()").get()
        if balcony:
            if "NON" in balcony:
                item_loader.add_value("balcony", False)
            elif "OUI" in balcony:
                item_loader.add_value("balcony", True)
        elevator = response.xpath("//div[span[.='Ascenseur']]/span[2]/text()").get()
        if elevator:
            if "NON" in elevator:
                item_loader.add_value("elevator", False)
            elif "OUI" in elevator:
                item_loader.add_value("elevator", True)
        parking = response.xpath("//div[span[.='Nombre de parking' or .='Nombre de garage']]/span[2]/text()").get()
        if parking:
            if "0" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        square_meters = response.xpath("//div[span[.='Surface habitable (m²)']]/span[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].split(",")[0])
      
        description = " ".join(response.xpath("//div[@class='detail-1__text']/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
       
        images = [response.urljoin(x) for x in response.xpath("//div[@class='swiper-wrapper js-lightbox-swiper']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        utilities = response.xpath("//div[span[contains(.,'Charges locatives')]]/span[2]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(",")[0])
        
        deposit = response.xpath("//div[span[.='Dépôt de garantie TTC']]/span[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ","").split(",")[0])
        rent = response.xpath("//div[span[contains(.,'Loyer CC* / mois')]]/span[2]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ","").split(",")[0])
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("landlord_name", "AGENCE SWEET HOME")
        item_loader.add_value("landlord_phone", "04 94 62 27 44")
        item_loader.add_value("landlord_email", "a.saintorens@agence-sweethome.com")
        yield item_loader.load_item()
