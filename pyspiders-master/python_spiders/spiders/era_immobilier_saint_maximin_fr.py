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
    name = 'era_immobilier_saint_maximin_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        headers = {
            'Connection': 'keep-alive',
            'Accept': '*/*',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.41 YaBrowser/21.2.0.1097 Yowser/2.5 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://www.era-immobilier-saint-maximin.fr',
            "Host":"www.era-immobilier-saint-maximin.fr",
            'Accept-Language': 'tr,en;q=0.9',
        }
        

        start_urls = [
            {"url": "https://www.era-immobilier-saint-maximin.fr/catalog/advanced_search_result_carto.php?action=update_search&ville=&map_polygone=&latlngsearch=&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=1&cfamille_id_tmp=1&cfamille_id_tmp=2&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Vente&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&30min=&C_30_MAX=&C_30_tmp=&check_C_28=Vente&C_33_search=EGAL&C_33_type=NUMBER&C_33_MIN=&C_33=&C_38_search=EGAL&C_38_type=NUMBER&C_38_MIN=&C_38=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30min=&30max=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_47_search=SUPERIEUR&C_47_type=NUMBER&C_46_search=SUPERIEUR&C_46_type=NUMBER&C_41_search=EGAL&C_41_type=NUMBER&C_50_search=SUPERIEUR&C_50_type=NUMBER&C_110_search=EGAL&C_110_type=NUMBER&C_1737_search=EGAL&C_1737_type=NUMBER&C_49_search=SUPERIEUR&C_49_type=NUMBER&C_48_search=SUPERIEUR&C_48_type=NUMBER&keywords=", "property_type" : "apartment", "prop_type" : "1"},

            {"url": "https://www.era-immobilier-saint-maximin.fr/catalog/advanced_search_result_carto.php?action=update_search&ville=&map_polygone=&latlngsearch=&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=2&cfamille_id_tmp=1&cfamille_id_tmp=2&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Vente&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&30min=&C_30_MAX=&C_30_tmp=&check_C_28=Vente&C_33_search=EGAL&C_33_type=NUMBER&C_33_MIN=&C_33=&C_38_search=EGAL&C_38_type=NUMBER&C_38_MIN=&C_38=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30min=&30max=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_47_search=SUPERIEUR&C_47_type=NUMBER&C_46_search=SUPERIEUR&C_46_type=NUMBER&C_41_search=EGAL&C_41_type=NUMBER&C_50_search=SUPERIEUR&C_50_type=NUMBER&C_110_search=EGAL&C_110_type=NUMBER&C_1737_search=EGAL&C_1737_type=NUMBER&C_49_search=SUPERIEUR&C_49_type=NUMBER&C_48_search=SUPERIEUR&C_48_type=NUMBER&keywords=", "property_type" : "house", "prop_type" : "2"}
            
        ]  # LEVEL 1
        for url in start_urls:
            data = {
                "jquery_aa_afunc":"call",
                "remote_function":"get_products_search_ajax_perso",
                "params[0][map_polygone]":"6.378057272213888,43.56801184080999/6.378057272213888,43.158167881806634/5.93775172779865,43.158167881806634/5.93775172779865,43.56801184080999",
                "params[0][action]":"",
                "params[0][C_28_search]":"EGAL",
                "params[0][C_28_type]":"UNIQUE",
                "params[0][C_28]":"Location,Saisonnier",
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
        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[contains(@class,'bien')]/div[contains(@class,'bien_details')]//h2/a/@href").extract():
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
        item_loader.add_value("external_source", "Era_Immobilier_Saint_Maximin_PySpider_france")
        item_loader.add_xpath("external_id", "//li[div[.='Référence']]/div[2]/text()")
        item_loader.add_xpath("title","//h2[@class='titre_bien']//text()") 
        energy_label = response.xpath("//div[@class='container-dpe col-md-6']//p[@class='classe_energie classe']//img/@src").get()
        if energy_label:
            energy = energy_label.split("dpe-")[-1].split(".")[0].upper()
            if energy in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label", energy)
        address = response.xpath("//span[@class='alur_location_ville']/text()").get()
        if address:
            item_loader.add_value("address", address) 
            item_loader.add_value("zipcode", address.split(" ")[0]) 
            item_loader.add_value("city", " ".join(address.split(" ")[1:])) 
     
        room_count = response.xpath("//li[div[.='Nbre de chambres']]/div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//li[div[.='Nbre de pièces']]/div[2]/text()")
        bathroom_count = response.xpath("//li[div[contains(.,'Salle d')]]/div[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        parking = response.xpath("//li[div[.='Parking']]/div[2]/text()").get()
        if parking:
            if parking.strip() == "0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        elevator = response.xpath("//li[div[.='Ascenseur']]/div[2]/text()").get()
        if elevator:
            if elevator.strip() == "0":
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        swimming_pool = response.xpath("//li[div[.='Piscine']]/div[2]/text()").get()
        if swimming_pool:
            if swimming_pool.strip() == "0":
                item_loader.add_value("swimming_pool", False)
            else:
                item_loader.add_value("swimming_pool", True)
        square_meters = response.xpath("//li[div[.='Surface habitable']]/div[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0])
      
        description = " ".join(response.xpath("//div[@class='description principale']/text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//ul[@class='slides']//li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        rent = response.xpath("//span[@class='alur_loyer_price']//text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.strip().replace(" ","").replace("\xa0",""))
  
        utilities = response.xpath("//span[@class='alur_location_charges']/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[-1].replace("\xa0",""))       
        deposit = response.xpath("//span[@class='alur_location_depot']/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[-1].replace("\xa0",""))

        landlord_name = response.xpath("//div[contains(@class,'contact_nego_details')]//p[@class='name']//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        landlord_phone = response.xpath("//div[@class='nego_rsac']/text()[contains(.,'RSAC')]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.split(":")[1].strip())
     
        yield item_loader.load_item()