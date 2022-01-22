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
import math

class MySpider(Spider):
    name = 'era_immobilier_compiegne_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    b_url = "https://www.era-immobilier-compiegne.fr"

    def start_requests(self):
        headers = {
            "Accept":"*/*",
            "Accept-Encoding":"gzip, deflate, br",
            "Accept-Language":"tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection":"keep-alive",
            "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
            "Host":self.b_url.split('://')[1],
            "Origin":f"{self.b_url}/",
            "X-Requested-With":"XMLHttpRequest",
        }

        start_urls = [
            {"url": f"{self.b_url}/catalog/advanced_search_result_carto.php?action=update_search&ville=&map_polygone=&latlngsearch=&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=1&cfamille_id_tmp=1&cfamille_id_tmp=2&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Vente&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&30min=&C_30_MAX=&C_30_tmp=&check_C_28=Vente&C_33_search=EGAL&C_33_type=NUMBER&C_33_MIN=&C_33=&C_38_search=EGAL&C_38_type=NUMBER&C_38_MIN=&C_38=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30min=&30max=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_47_search=SUPERIEUR&C_47_type=NUMBER&C_46_search=SUPERIEUR&C_46_type=NUMBER&C_41_search=EGAL&C_41_type=NUMBER&C_50_search=SUPERIEUR&C_50_type=NUMBER&C_110_search=EGAL&C_110_type=NUMBER&C_1737_search=EGAL&C_1737_type=NUMBER&C_49_search=SUPERIEUR&C_49_type=NUMBER&C_48_search=SUPERIEUR&C_48_type=NUMBER&keywords=", 
             "property_type" : "apartment", 
              "prop_type" : "1"},

            {"url": f"{self.b_url}/catalog/advanced_search_result_carto.php?action=update_search&ville=&map_polygone=&latlngsearch=&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=2&cfamille_id_tmp=1&cfamille_id_tmp=2&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Vente&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&30min=&C_30_MAX=&C_30_tmp=&check_C_28=Vente&C_33_search=EGAL&C_33_type=NUMBER&C_33_MIN=&C_33=&C_38_search=EGAL&C_38_type=NUMBER&C_38_MIN=&C_38=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30min=&30max=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_47_search=SUPERIEUR&C_47_type=NUMBER&C_46_search=SUPERIEUR&C_46_type=NUMBER&C_41_search=EGAL&C_41_type=NUMBER&C_50_search=SUPERIEUR&C_50_type=NUMBER&C_110_search=EGAL&C_110_type=NUMBER&C_1737_search=EGAL&C_1737_type=NUMBER&C_49_search=SUPERIEUR&C_49_type=NUMBER&C_48_search=SUPERIEUR&C_48_type=NUMBER&keywords=", 
             "property_type" : "house", 
              "prop_type" : "2"}
        ]

        for url in start_urls:
            data = {
                "jquery_aa_afunc":"call",
                "remote_function":"get_products_search_ajax_perso",
                "params[0][map_polygone]":"-0.3130681184533001,43.340570488571956/-0.3130681184533001,43.26286582173947/-0.3978930340614949,43.26286582173947/-0.3978930340614949,43.340570488571956",
                "params[0][action]":"",
                "params[0][C_28_search]":"EGAL",
                "params[0][C_28_type]":"UNIQUE",
                "params[0][C_28]":"Vente",
                "params[0][cfamille_id_search]":"EGAL",
                "params[0][cfamille_id_type]":"TEXT",
                "params[0][cfamille_id]": "{}".format(url.get('prop_type')),
                "params[0][C_30_search]":"COMPRIS",
                "params[0][C_30_type]":"NUMBER",
                "params[0][C_30_MIN]":"",
                "params[0][C_30_MAX]":"",
                "params[0][C_47_search]":"SUPERIEUR",
                "params[0][C_47_type]":"NUMBER",
                "params[0][C_47]":"",
                "params[0][C_46_search]":"SUPERIEUR",
                "params[0][C_46_type]":"NUMBER",
                "params[0][C_46]":"",
                "params[0][C_41_search]":"SUPERIEUR",
                "params[0][C_41_type]":"NUMBER",
                "params[0][C_41]":"",
                "params[0][C_50_search]":"SUPERIEUR",
                "params[0][C_50_type]":"NUMBER",
                "params[0][C_50]":"",
                "params[0][C_110_search]":"EGAL",
                "params[0][C_110_type]":"FLAG",
                "params[0][C_110]":"",
                "params[0][C_1737_search]":"EGAL",
                "params[0][C_1737_type]":"FLAG",
                "params[0][C_1737]":"",
                "params[0][C_49_search]":"SUPERIEUR",
                "params[0][C_49_type]":"NUMBER",
                "params[0][C_49]":"",
                "params[0][C_48_search]":"SUPERIEUR",
                "params[0][C_48_type]":"NUMBER",
                "params[0][C_48]":"",
                "params[0][C_34_search]":"COMPRIS",
                "params[0][C_34_type]":"NUMBER",
                "params[0][C_34_MIN]":"",
                "params[0][admin_id]":"",
                "params[]": "true",
                "params[]": "400",
                "params[]":"map_carto",
                "params[]": "../templates/era-agences/catalog/images/marker2.png",
                "params[]":"",
                "params[]":"../templates/era-agences/catalog/images/marker.png",
                "params[]":"false",
                "params[]":"../templates/era-agences/catalog/images/marker_cluster.png",
            }
            yield Request(url=url.get('url'), callback=self.parse, method="POST", headers=headers, body=json.dumps(data), meta={'property_type': url.get("property_type")})
    
    # 1. FOLLOWING
    def parse(self, response):

        seen = False
        for item in response.xpath("//div[@id='listing_bien']/div//h2/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get("property_type")})
            seen = True
        
        pagination = response.xpath("//ul[@class='pagination']/li/a[@class='page_suivante']/@href").get()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse, meta={'property_type': response.meta.get("property_type")})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Era_Immobilier_Compiegne_PySpider_france")
        
        title = response.xpath("//h2/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        square_meters = response.xpath("//img[contains(@src,'surface')]/following-sibling::p/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())
        
        room_count = response.xpath("//img[contains(@src,'chambre')]/following-sibling::p/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li/div[contains(.,'pièce')]/following-sibling::div/text()[.!='0']").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li/div[contains(.,'Salle')]/following-sibling::div/text()[.!='0']").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = response.xpath("//span[@class='alur_loyer_price']/text()").get()
        if rent:
            price = rent.split("€")[0].split("Loyer")[1].replace("\u00a0","").strip()
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        address = response.xpath("//span[@class='alur_location_ville']/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(" ")[0]
            city = address.split(zipcode)[1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        external_id = response.xpath("//li/div[contains(.,'Référence')]/following-sibling::div/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        terrace = response.xpath("//li/div[contains(.,'Terrasse')]/following-sibling::div/text()[.!='0']").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        balcony = response.xpath("//li/div[contains(.,'Balcon')]/following-sibling::div/text()[.!='0']").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        swimming_pool = response.xpath("//li/div[contains(.,'Piscine')]/following-sibling::div/text()[.!='0']").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        elevator = response.xpath("//li/div[contains(.,'Ascenseur')]/following-sibling::div/text()[.!='0']").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        parking = response.xpath("//li/div[contains(.,'Parking') or contains(.,'Garage')]/following-sibling::div/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        
        description = " ".join(response.xpath("//div[contains(@class,'description principale')]/text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [response.urljoin(x) for x in response.xpath("//ul[@class='slides']//@href").getall()]
        if images:
            item_loader.add_value("images", images)
            
        energy_label = response.xpath("//img/@src[contains(.,'dpe-ges/dpe')]").get()
        if energy_label:
            energy_label = energy_label.split("/dpe-")[1].split(".")[0]
            item_loader.add_value("energy_label", energy_label.upper())
        
        utilities = response.xpath("//span[@class='alur_location_charges']/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].split(":")[1].replace("\u00a0","").strip()
        item_loader.add_value("utilities", utilities)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        name = response.xpath("//p[@class='name']/text()").get()
        if name:
            item_loader.add_value("landlord_name", name.strip())

        phone = response.xpath("//div[contains(@class,'contact_nego_boutons_return')]/a/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        
        item_loader.add_value("landlord_email", "mentions-legales@erafrance.com")
        
        yield item_loader.load_item()