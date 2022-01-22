# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import math

class MySpider(Spider):
    name = 'immotys_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.immotys.fr/catalog/advanced_search_result_carto.php?action=update_search&search_id=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_REPLACE=2&C_27_search=EGAL&C_27_type=UNIQUE&C_27=2&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_30_MIN=0&keywords=&map_polygone=&C_65_REPLACE=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=",
                "property_type" : "house"
            },
            {
                "url" : "https://www.immotys.fr/catalog/advanced_search_result_carto.php?action=update_search&search_id=1681783564561215&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_REPLACE=1&C_27_search=EGAL&C_27_type=UNIQUE&C_27=1&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_30_MIN=0&keywords=&map_polygone=&C_65_REPLACE=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=",
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//span[@class='nbPhotos']/../@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        
        next_page = response.xpath("//a[@class='page_suivante']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")}
            )

        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_value("external_source", "Immotys_PySpider_"+ self.country + "_" + self.locale)
   
        price = response.xpath("//div[@class='prix loyer']/span[@class='alur_loyer_price']//text()").extract_first()
        if price:
            rent=price.split("Loyer")[1].split("/")[0]
            item_loader.add_value("rent_string", rent.replace("\xa0",".").strip())
        
        external_id = response.xpath("//div[contains(@class,'description')]/div/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Ref")[1].strip())
 
        item_loader.add_xpath("floor", "//div//li//div[contains(.,'Etage') and not(contains(.,'Dernier Etage'))]/following-sibling::div//text()")
        item_loader.add_xpath("zipcode","//div//li//div[contains(.,'Code postal')]/following-sibling::div//text()")
 
        item_loader.add_xpath("city", "//div//li//div[contains(.,'Ville')]/following-sibling::div//text()")
        item_loader.add_xpath("address","//div//li//div[contains(.,'Ville')]/following-sibling::div//text()")

        square = response.xpath("//div[contains(@class,'carac')]/div[contains(.,'m')]/text()").extract_first()
        if square:
            square_meters=square.split("m")[0]
            square_meters = math.ceil(float(square_meters.strip()))
            item_loader.add_value("square_meters",square_meters )
       
        room_count = response.xpath("//div[contains(@class,'carac')]/div[contains(.,'Pièce')]/text()").extract_first()
        if room_count:
            room_count=room_count.split("Pièce")[0]
            item_loader.add_value("room_count",room_count.strip() )

        bathroom_count = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Salle(s)')]/following-sibling::div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("LatLng(")[1].split(",")[0]
            longitude = latitude_longitude.split("LatLng(")[1].split(",")[1].split(")")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        desc = "".join(response.xpath("//div[contains(@class,'description')]/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
       

        utilities = response.xpath("//div//li//div[contains(.,'Provision sur charges')]/following-sibling::div//text()").extract_first()
        charges = response.xpath("//span[contains(@class,'honos')]/text()").get()
        if charges:
            charges = charges.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", charges)
        elif utilities:
            item_loader.add_value("utilities", utilities.replace("EUR","").strip())
        
        deposit = response.xpath("//div//li//div[contains(.,'Dépôt de Garantie')]/following-sibling::div//text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.split("EUR")[0].strip()) 
        energy_label = response.xpath("//div//li//div[contains(.,'Conso Energ')]/following-sibling::div//text()").extract_first()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())

        parking = response.xpath("//div//li//div[contains(.,'parking')]/following-sibling::div//text()").extract_first()
        garage = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Stationnement')]/following-sibling::div//text()").get()
        if parking or garage:
            if garage:
                item_loader.add_value("parking", True)
            elif "Non" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        
        terrace = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'terrasses')]/following-sibling::div//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        furnished = response.xpath("//div//li//div[contains(.,'Meublé')]/following-sibling::div//text()").extract_first()
        if furnished:
            if "Non" in furnished:
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        elevator = response.xpath("//div//li//div[contains(.,'Ascenseur')]/following-sibling::div//text()").extract_first()
        if elevator:
            if "Non" in elevator:
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//div//li//div[contains(.,'balcons')]/following-sibling::div//text()").extract_first()
        if balcony:
            if "Non" in balcony:
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)
        
        item_loader.add_value("landlord_phone", "02 23 55 28 40")
        item_loader.add_value("landlord_name", "IMMOTYS")
        item_loader.add_value("landlord_email", "agence@immotys.fr")
        img=response.xpath("//div[@class='diapoDetail']/div/@style").extract() 
        if img:
            images=[]
            for x in img:
                image=x.split("('")[1].split("')")[0]
                images.append(image)
            if images:
                item_loader.add_value("images",  list(set(images)))

        item_loader.add_value("landlord_phone", "02.23.55.28.40")
        item_loader.add_value("landlord_email", "agence@immotys.fr")
        item_loader.add_value("landlord_name", "IMMOTYS")
        yield item_loader.load_item()
