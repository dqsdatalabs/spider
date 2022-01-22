# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest 
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
from  geopy.geocoders import Nominatim 
from html.parser import HTMLParser
import re

class MySpider(Spider):
    name = 'immobilier47_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Immobilier47_PySpider_france_fr"
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.immobilier47.com/catalog/advanced_search_result.php?action=update_search&search_id=1698360553801120&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_30_MIN=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_34_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&C_38_MAX=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_47_type=NUMBER&C_47_search=COMPRIS&C_47_MIN=&C_94_type=FLAG&C_94_search=EGAL&C_94=", "property_type": "house"},
            {"url": "https://www.immobilier47.com/catalog/advanced_search_result.php?action=update_search&search_id=1698360553801120&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_30_MIN=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_34_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&C_38_MAX=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_47_type=NUMBER&C_47_search=COMPRIS&C_47_MIN=&C_94_type=FLAG&C_94_search=EGAL&C_94=", "property_type": "apartment"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'item-product')]//div[contains(@class,'visuel-product')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rented = response.xpath("//div[@class='vertical-pano']/img/@class").extract_first()
        if rented:
            return

        title = "".join(response.xpath("//h1/text()").extract())
        title2 = title.replace('\r', '').replace('\n', '').strip()
        item_loader.add_value("title", re.sub("\s{2,}", " ", title2))
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("search_id=")[1].split("&")[0])


        item_loader.add_value("external_source", self.external_source)
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('LatLng(')[1].split(",")[1].split(')')[0].strip().replace(',', '.') 
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)


        zipcode = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Code Postal')]//following-sibling::div//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        address = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Ville')]//following-sibling::div//text()").get()
        if address:          
            item_loader.add_value("address", address+" "+zipcode)
            item_loader.add_value("city", address)
        adrescheck=item_loader.get_output_value("address")
        if not adrescheck:
            adres=response.xpath("//div[@class='title-product']/h1/span/text()").get()
            if adres:
                item_loader.add_value("address",adres)
            city=adres.split(" ")[-1]
            if city:
                item_loader.add_value("city",city)
            zipcode=adres.split(" ")[0]
            if zipcode:
                item_loader.add_value("zipcode",zipcode)

        square_meters = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Surface')]//following-sibling::div//text()").get()
        if square_meters:
            square_meters = square_meters.split('m')[0].strip().replace(',', '.').strip()
            square_meters = str(math.ceil(float(square_meters)))
            item_loader.add_value("square_meters", square_meters)

        bathroom = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Salle')]//following-sibling::div//text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip())
        
        utilities = response.xpath("//span[contains(@class,'charges')]//text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(": ")[1].strip().split(" ")[0].strip())


        room_count = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'pièces')]//following-sibling::div//text()").get()
        if room_count:
            room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//span[contains(@class,'price_alur_description')]//span[contains(@class,'alur_loyer_price')]//text()").get()
        if rent:
            rent = rent.split("€")[0].strip().split(" ")[-1]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", 'EUR')

        desc = " ".join(response.xpath("//div[contains(@class,'description-product')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        images = [x for x in response.xpath("//div[contains(@class,'container-slider-product')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        deposit = response.xpath("//span[contains(@class,'depot')]//text()").get()
        if deposit:
            deposit = deposit.split(':')[1].split('€')[0].strip().replace(' ', '').replace(',', '').replace('.', '')
            item_loader.add_value("deposit", deposit)

        energy_label = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Conso Energ')]//following-sibling::div//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        floor = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Etage')]//following-sibling::div//text()").get()
        if floor:
            item_loader.add_value("floor", floor)

        swimming_pool = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Piscine')]//following-sibling::div//text()[contains(.,'Oui')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        terrace = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Terrasse')]//following-sibling::div//text()[contains(.,'Oui')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        landlord_name = response.xpath("//div[contains(@class,'nego-name')]//text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//div[contains(@class,'nego-product')]//a[contains(@href,'tel')]//@href").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone.split(":")[-1])

        landlord_email = response.xpath("//div[contains(@class,'nego-product')]//a[contains(@href,'mailto')]//@href").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.split(":")[-1])

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data