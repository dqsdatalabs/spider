# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime
import math

class MySpider(Spider):
    name = 'letucimmobilierchatillon_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Letucimmobilierchatillon_PySpider_france_fr"
    post_url = "https://www.letuc-immobilier-chatillon.com/recherche/"
    def start_requests(self):
        formdata = {
            "data[Search][offredem]": "2",
            "data[Search][idtype][]": "2",
            "data[Search][prixmax]": "",
            "data[Search][prixmin]": "",
            "data[Search][surfmax]": "",
            "data[Search][surfmin]": "",
            "data[Search][piecesmax]": "",
            "data[Search][piecesmin]": "",
            "data[Search][NO_DOSSIER]": "",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            dont_filter=True,
            formdata=formdata,
            meta={
                "property_type":"apartment",
            }
        )


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='property__content-wrapper']//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_xpath("title", "//h1[@class='title__content']//span[1]/text()")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        price = response.xpath("//div[@class='main-info__price']/span[1]/text()").extract_first()
        if price:
            item_loader.add_value("rent_string", price.replace(" ","."))

        external_id = response.xpath("//span[@class='detail-1__reference-number']//text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        zipcode = response.xpath("//div[span[.='Code postal']]/span[2]//text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        city = response.xpath("//div[span[contains(.,'Ville')]]/span[2]//text()").extract_first()
        if city:
            item_loader.add_value("city", city.strip())

        floor = response.xpath("//div[span[contains(.,'Etage')]]/span[2]//text()").extract_first()
        if floor:
            item_loader.add_value("floor", floor.strip())
 
        item_loader.add_xpath("address","//h1[@class='title__content']//span[2]/text()")
        item_loader.add_xpath("room_count","//div[span[contains(.,'pièces')]]/span[2]//text()")
        item_loader.add_xpath("bathroom_count","//div[span[contains(.,'Nb de salle de')]]/span[2]//text()")

        square = response.xpath("//div[span[contains(.,'habitable ')]]/span[2]//text()").extract_first()
        if square:
            square =square.split("m")[0].strip().replace(",",".")
            square_meters = math.ceil(float(square.strip()))
            item_loader.add_value("square_meters",square_meters )
        
        # a_date = response.xpath("//strong[contains(.,'Disponibilité')]/following-sibling::p/text()").extract_first()
        # if a_date:
        #     datetimeobject = datetime.strptime(a_date,'%d/%m/%Y')
        #     newformat = datetimeobject.strftime('%Y-%m-%d')
        #     item_loader.add_value("available_date", newformat)
               
        desc = "".join(response.xpath("//div[@class='detail-1__container-text']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
 
        deposit = response.xpath("//div[span[contains(.,'de garantie')]]/span[2]//text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ","").strip())
        utilities = response.xpath("//div[span[contains(.,'Charges locatives ')]]/span[2]//text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.replace(" ","").strip()) 
        images = [response.urljoin(x) for x in response.xpath("//div[@class='swiper-wrapper js-lightbox-swiper']//a//img/@data-src").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "01 46 12 04 80") 
        item_loader.add_value("landlord_email", "chatillon.letuc@acilimmo.fr") 
        item_loader.add_value("landlord_name", "LE TUC CHATILLON")
  
        furnished = response.xpath("//div[span[contains(.,'Meublé')]]/span[2]//text()").extract_first()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//div[span[contains(.,'Ascenseur')]]/span[2]//text()").extract_first()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//div[span[contains(.,'Balcon')]]/span[2]//text()").extract_first()
        if balcony:
            if "Non" in balcony.lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)
        terrace = response.xpath("//div[span[contains(.,'Terrasse')]]/span[2]//text()").extract_first()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)

        parking = response.xpath("//div[span[contains(.,'garage')]]/span[2]//text()").extract_first()
        if parking:
            if "Non" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        
        yield item_loader.load_item()