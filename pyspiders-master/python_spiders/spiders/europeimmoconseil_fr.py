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
    name = 'europeimmoconseil_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.europeimmoconseil.fr/catalog/advanced_search_result.php?action=update_search&search_id=1689937555792930&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.europeimmoconseil.fr/catalog/advanced_search_result.php?action=update_search&search_id=1689937555792930&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2%2C17&C_27_tmp=2&C_27_tmp=17&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=",
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

        for item in response.xpath("//div[@id='listing_bien']/div"):
            follow_url = response.urljoin(item.xpath("./div[1]/a/@href").get())
            is_studio = item.xpath(".//h2/a/text()[contains(.,'Studio') or contains(.,'studio') or contains(.,'STUDIO')]").get()
            if is_studio: yield Request(follow_url, callback=self.populate_item, meta={"property_type":"studio"})
            else: yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[@class='page_suivante']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Europeimmoconseil_PySpider_france")
        
        item_loader.add_xpath("title", "//div[@id='content_intro_header']//h3//text()")      
        zipcode = response.xpath("//li/div[div[.='Code Postal Internet']]/div[2]//text()").extract_first()
        city = response.xpath("substring-after(//li/span[contains(.,'Ville :')]/text(),':')").extract_first()      
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
                
        external_id = response.xpath("//li/span[contains(.,'Référence :')]/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip()) 
        room_count = response.xpath("//li/div[div[.='Chambres']]/div[2]//text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        else:
            room_count = response.xpath("//li/div[div[.='Nombre pièces']]/div[2]//text()").extract_first()
            if room_count:
                item_loader.add_value("room_count",room_count.strip())
   
        rent = response.xpath("//div[@class='formatted_price_alur2_div']//span[@class='alur_loyer_price']").extract_first()
        if rent:    
            item_loader.add_value("rent_string",rent.replace(" ","").replace('\xa0', ''))  
           
        square = response.xpath("//li/div[div[.='Surface']]/div[2]//text()").extract_first()
        if square:
            square_meters = square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 
        deposit = response.xpath("//li/div[div[.='Dépôt de Garantie']]/div[2]//text()").extract_first()
        if deposit:
            item_loader.add_value("deposit",int(float(deposit.split("EUR")[0].strip().replace(",",".")))) 
       
        parking = response.xpath("//li/div[div[.='Type de Stationnement' or .='Nombre places parking']]/div[2]//text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
        terrace = response.xpath("//li/div[div[.='Nombre de terrasses']]/div[2]//text()").extract_first()    
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
        balcony = response.xpath("//li/div[div[contains(.,'Nombre balcons')]]/div[2]//text()").extract_first()  
        if balcony:
            item_loader.add_value("balcony", True) 
        desc = " ".join(response.xpath("//div[contains(@class,'content_details_description')]/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
     
        images = [response.urljoin(x) for x in response.xpath("//div[@id='flex_slider_bien']//ul[@class='slides']/li/a/@href").extract()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "EUROPE IMMO CONSEIL")
        item_loader.add_value("landlord_phone", "04.22.89.01.50")

        yield item_loader.load_item()