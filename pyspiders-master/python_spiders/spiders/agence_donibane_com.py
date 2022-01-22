# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'agence_donibane_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agence-donibane.com/catalog/advanced_search_result.php?action=update_search&search_id=&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=0&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agence-donibane.com/catalog/advanced_search_result.php?action=update_search&search_id=1689935038180965&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=0&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='listing_bien']/div[@class='item-listing']"):
            follow_url = response.urljoin(item.xpath(".//div[@class='infos-product']/a/@href").get())
            rented = item.xpath(".//span[contains(.,'Loué')]").get()
            if not rented: yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//li[contains(@class,'next-link')]/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Agence_Donibane_PySpider_france")
        
        item_loader.add_xpath("title", "//div[@class='infos-products-header']/h1//text()")      
        zipcode = response.xpath("//li/div[div[.='Code postal']]/div[2]//text()").extract_first()
        city = response.xpath("//li/div[div[.='Ville']]/div[2]//text()").extract_first()      
        address = response.xpath("//span[@class='alur_location_ville']//text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip()) 
            if not zipcode:
                zipcode = address.strip().split(" ")[0]
            if not city:
                city = " ".join(address.strip().split(" ")[1:])
        if city:
            item_loader.add_value("city",city.strip()) 
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip()) 
        item_loader.add_xpath("floor", "//li/div[div[.='Etage']]/div[2]//text()")
        item_loader.add_xpath("energy_label", "//li/div[div[contains(.,'Conso Energ')]]/div[2]//text()[.!='Vierge']")
        item_loader.add_xpath("bathroom_count", "//li/div[div[contains(.,'Salle(s) d')]]/div[2]//text()")
                
        external_id = response.xpath("//li/span[contains(.,'Ref. :')]/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip()) 
        room_count = response.xpath("//li/div[div[.='Chambres']]/div[2]//text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        else:
            room_count = response.xpath("//li/div[div[.='Nombre pièces']]/div[2]//text()").extract_first()
            if room_count:
                item_loader.add_value("room_count",room_count.strip())
   
        rent = response.xpath("//div[@class='infos-products-header']//span[@class='alur_loyer_price']/text()").get()
        if rent:    
            item_loader.add_value("rent",re.sub(r'\D', '', rent))  
            item_loader.add_value("currency", "EUR")
        square = response.xpath("//li/div[div[.='Surface']]/div[2]//text()").extract_first()
        if square:
            square_meters = square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 
        deposit = response.xpath("//li/div[div[.='Dépôt de Garantie']]/div[2]//text()").extract_first()
        if deposit:
            item_loader.add_value("deposit",int(float(deposit.split("EUR")[0].strip().replace(",",".")))) 
        utilities = response.xpath("//li/div[div[contains(.,' charges') and not(contains(.,'Loyer'))]]/div[2]//text()").extract_first()
        if utilities:
            item_loader.add_value("utilities",int(float(utilities.split("EUR")[0].strip().replace(",",".")))) 
        
        available_date = response.xpath("//li/div[div[contains(.,'Date de disponibilité')]]/div[2]//text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        parking = response.xpath("//li/div[div[.='Nombre places parking' or .='Nombre garages/Box']]/div[2]//text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
        terrace = response.xpath("//li/div[div[.='Surface terrasse']]/div[2]//text()").extract_first()    
        if terrace:
            item_loader.add_value("terrace", True)
        furnished =response.xpath("//li/div[div[.='Meublé']]/div[2]//text()").extract_first()  
        if furnished:
            if furnished.upper().strip() =="NON":
                item_loader.add_value("furnished", False)
            elif furnished.upper().strip() == "OUI":
                item_loader.add_value("furnished", True)
        elevator =response.xpath("//li/div[div[contains(.,'Ascenseur')]]/div[2]//text()").extract_first()  
        if elevator:
            if elevator.upper().strip() =="NON":
                item_loader.add_value("elevator", False)
            elif elevator.upper().strip() == "OUI":
                item_loader.add_value("elevator", True)
        swimming_pool =response.xpath("//li/div[div[contains(.,'Piscine')]]/div[2]//text()").extract_first()  
        if swimming_pool:
            if swimming_pool.upper().strip() =="NON":
                item_loader.add_value("swimming_pool", False)
            elif swimming_pool.upper().strip() == "OUI":
                item_loader.add_value("swimming_pool", True)
        balcony =response.xpath("//li/div[div[contains(.,'Balcon ')]]/div[2]//text()").extract_first()  
        if balcony:
            item_loader.add_value("balcony", True) 
        desc = " ".join(response.xpath("//div[@class='product-description']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
     
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider_product']/div/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        landlord_name =response.xpath("//div[@class='name-agence']//text()").extract_first()  
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.upper())
        item_loader.add_xpath("landlord_phone", "//div[@class='link-contact']//span/@data-content")
        yield item_loader.load_item()