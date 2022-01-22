# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser
import re
import math
from datetime import datetime

class MySpider(Spider):
    name = 'eraimmovalie_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):

        start_urls = [
            {
                "url": [
                    "https://www.immobilier-elancourt-la-clef-era.fr/catalog/advanced_search_result_carto.php?action=update_search&map_polygone=&C_27_search=CONTIENT&C_27_type=TEXT&C_27=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location%2CSaisonnier&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=1&cfamille_id_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30=0&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_30max=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.immobilier-elancourt-la-clef-era.fr/catalog/advanced_search_result_carto.php?action=update_search&map_polygone=&C_27_search=CONTIENT&C_27_type=TEXT&C_27=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location%2CSaisonnier&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=2&cfamille_id_tmp=2&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30=0&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_30max=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN="
                ],
                "property_type": "house"
            }
            
        ]  # LEVEL 1
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@id='listing_bien']/div"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            print(follow_url)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get("property_type")})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h2/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        item_loader.add_value("property_type", response.meta.get("property_type"))
        
        item_loader.add_value("external_source", "Eraimmovalie_PySpider_"+ self.country + "_" + self.locale)

        rent="".join(response.xpath(
            "//span[@class='alur_loyer_price']/text()").get())
        if rent:
            rent = rent.replace("\xa0", "").split("€")[0].strip().split(" ")[-1]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        
        square_meters=response.xpath(
            "//li[div[contains(.,'habitable')]]/div[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m²')[0])
        
        
        room_count=response.xpath(
            "//li[div[contains(.,'chambre')]]/div[2]/text() | //li[div[contains(.,'pièces')]]/div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count=response.xpath(
            "//li[div[contains(.,'Salle')]]/div[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        address = response.xpath("//span[@class='alur_location_ville']/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(" ")[0]
            city = address.split(zipcode)[1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            
        external_id=response.xpath(
            "//li[div[contains(.,'Référence')]]/div[2]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
            
        desc="".join(response.xpath("//div[contains(@class,'description ')]/text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        utilities = response.xpath("//span[@class='alur_location_charges']/text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        elevator=response.xpath(
            "//li[div[contains(.,'Ascenseur')]]/div[2]/text()").get()
        if elevator!='0':
            item_loader.add_value("elevator",True)
        
        balcony=response.xpath(
            "//li[div[contains(.,'Balcon')]]/div[2]/text()").get()
        if balcony!='0':
            item_loader.add_value("balcony",True)
        
        terrace=response.xpath("//li[div[contains(.,'Terrasse')]]/div[2]/text()").get()
        if terrace!='0':
            item_loader.add_value("terrace",True)
        
        parking=response.xpath(
            "//li[div[contains(.,'Parking')]]/div[2]/text() | //li[div[contains(.,'Garage')]]/div[2]/text()").get()
        if parking!='0':
            item_loader.add_value("parking",True)
        deposit=response.xpath("//span[@class='alur_location_depot']/text()").get()
        if deposit:
            deposit=deposit.split(":")[-1].split("€")[0].replace(" ","").replace("\xa0","")
            item_loader.add_value("deposit",deposit)
        
        energy_label = response.xpath("//div[contains(@class,'container-dpe')]//@src[contains(.,'DPE')]").get()
        if energy_label:
            energy_label = energy_label.split("_")[-2]
            item_loader.add_value("energy_label", energy_label)
        
        
        images=[x for x in response.xpath("//ul[@class='slides']//@href").getall()]    
        for image in images:
            item_loader.add_value("images", image.strip())
            
        landlord_name = response.xpath("//p[@class='name']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
       
        phone="".join(response.xpath("//div[@class='contact_nego_boutons_return']/a//text()").get())
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        email=response.xpath("//a[contains(@href,'mailto')]/text()").get()
        if email:
            item_loader.add_value("landlord_email",email) 
        
        yield item_loader.load_item()