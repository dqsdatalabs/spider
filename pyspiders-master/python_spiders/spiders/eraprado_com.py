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
    name = 'eraprado_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Eraprado_PySpider_france_fr'
    def start_requests(self):
        headers = {
            "Accept":"*/*",
            "Accept-Encoding":"gzip, deflate, br",
            "Accept-Language":"tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection":"keep-alive",
            "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
            "Host":"www.immobilier-marseille-08-prado-era.fr",
            "Origin":"https://www.immobilier-marseille-08-prado-era.fr/",
            "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36",
            "X-Requested-With":"XMLHttpRequest",
        }

        start_urls = [
            {"url": "https://www.immobilier-marseille-08-prado-era.fr/catalog/advanced_search_result_carto.php?action=update_search&ville=&map_polygone=&latlngsearch=&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=1&cfamille_id_tmp=1&cfamille_id_tmp=2&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Vente&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&30min=&C_30_MAX=&C_30_tmp=&check_C_28=Vente&C_33_search=EGAL&C_33_type=NUMBER&C_33_MIN=&C_33=&C_38_search=EGAL&C_38_type=NUMBER&C_38_MIN=&C_38=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30min=&30max=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_47_search=SUPERIEUR&C_47_type=NUMBER&C_46_search=SUPERIEUR&C_46_type=NUMBER&C_41_search=EGAL&C_41_type=NUMBER&C_50_search=SUPERIEUR&C_50_type=NUMBER&C_110_search=EGAL&C_110_type=NUMBER&C_1737_search=EGAL&C_1737_type=NUMBER&C_49_search=SUPERIEUR&C_49_type=NUMBER&C_48_search=SUPERIEUR&C_48_type=NUMBER&keywords=", "property_type" : "apartment", "prop_type" : "1"},

            {"url": "https://www.immobilier-marseille-08-prado-era.fr/catalog/advanced_search_result_carto.php?action=update_search&ville=&map_polygone=&latlngsearch=&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=2&cfamille_id_tmp=1&cfamille_id_tmp=2&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Vente&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&30min=&C_30_MAX=&C_30_tmp=&check_C_28=Vente&C_33_search=EGAL&C_33_type=NUMBER&C_33_MIN=&C_33=&C_38_search=EGAL&C_38_type=NUMBER&C_38_MIN=&C_38=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30min=&30max=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_47_search=SUPERIEUR&C_47_type=NUMBER&C_46_search=SUPERIEUR&C_46_type=NUMBER&C_41_search=EGAL&C_41_type=NUMBER&C_50_search=SUPERIEUR&C_50_type=NUMBER&C_110_search=EGAL&C_110_type=NUMBER&C_1737_search=EGAL&C_1737_type=NUMBER&C_49_search=SUPERIEUR&C_49_type=NUMBER&C_48_search=SUPERIEUR&C_48_type=NUMBER&keywords=", "property_type" : "house", "prop_type" : "2"}
            
            
        ]  # LEVEL 1
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
        for item in response.xpath("//div[@id='listing_bien']/div"):
            follow_url = response.urljoin(item.xpath(".//h2/a/@href").get().split("?")[0])
            address = item.xpath(".//span[@class='bien_ville']/text()").get()
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get("property_type"), "address":address})
            seen = True
        
        pagination = response.xpath("//ul[@class='pagination']/li/a[@class='page_suivante']/@href").get()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse, meta={'property_type': response.meta.get("property_type")})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("property_type", response.meta.get("property_type"))
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Eraprado_PySpider_"+ self.country + "_" + self.locale)

        rent="".join(response.xpath("//div[@class='prix loyer']/span[1]/text()").getall())
        if rent:
            price=rent.replace("\xa0","")
            item_loader.add_value("rent_string", price)
        
        square_meters=response.xpath(
            "//div[@class='criteres-list']/ul/li/div[contains(.,'Surface')]//following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m²')[0])
        
        room_count=response.xpath(
            "//div[@class='criteres-list']/ul/li/div[contains(.,'pièces')]//following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        # latitude_longitude = response.xpath("//script[contains(.,'myLatlng')]/text()").get()
        address = response.meta.get("address")
        item_loader.add_value("address", address)

        city = response.xpath("//meta[@property='og:url']/@content").get()
        if city:
            city = city.split("immobilier-")[1].split("-")[0].strip()
            item_loader.add_value("city", city)
        
        # if latitude_longitude:
        #     latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
        #     longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0]
        #     item_loader.add_value("longitude", longitude)
        #     item_loader.add_value("latitude", latitude)
        #     geolocator = Nominatim(user_agent=response.url)
        #     try:
        #         location = geolocator.reverse(latitude + ', ' + longitude, timeout=None)
        #         if location.address:
        #             address = location.address
        #             if location.raw['address']['postcode']:
        #                 zipcode = location.raw['address']['postcode']
        #     except:
        #         address = None
        #         zipcode = None
        #     if address:
        #         item_loader.add_value("address", address)
        #         item_loader.add_value("zipcode", zipcode)
        #         item_loader.add_value("longitude", longitude)
        #         item_loader.add_value("latitude", latitude)
        
        bath_room = response.xpath("//div[@class='criteres-list']/ul/li/div[contains(.,'Salle de bain') or contains(.,'Salle d')]//following-sibling::div/text()").get()
        if bath_room:
            item_loader.add_value("bathroom_count", bath_room)

        external_id=response.xpath(
            "//div[@class='criteres-list']/ul/li/div[contains(.,'Référence')]//following-sibling::div/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
            
        desc="".join(response.xpath("//div[contains(@class,'description principale')]/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.strip())

            if "meublé" in desc.lower():
                item_loader.add_value("furnished", True)
        
        deposit = response.xpath("//span[contains(@class,'price_alur_description')]//span[@class='alur_location_depot']/text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip().replace("\xa0", "")
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//span[contains(@class,'price_alur_description')]//span[@class='alur_location_honos']/text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip().replace("\xa0", "")
            item_loader.add_value("utilities", utilities)
        
        if not item_loader.get_collected_values("furnished"):
            furnished = response.xpath("//span[@class='price_alur_description']//text()[contains(.,'meublé')]").get()
            if furnished:
                item_loader.add_value("furnished", True)
            
        elevator=response.xpath(
            "//div[@class='criteres-list']/ul/li/div[contains(.,'Ascenseur')]//following-sibling::div/text()").get()
        if elevator!='0':
            item_loader.add_value("elevator",True)
            
        balcony=response.xpath(
            "//div[@class='criteres-list']/ul/li/div[contains(.,'balcony')]//following-sibling::div/text()").get()
        if balcony!='0':
            item_loader.add_value("balcony",True)
        
        terrace=response.xpath("//div[@class='criteres-list']/ul/li/div[contains(.,'Terrasse')]//following-sibling::div/text()").get()
        if terrace!='0':
            item_loader.add_value("terrace",True)
            
        swimming_pool=response.xpath("//div[@class='criteres-list']/ul/li/div[contains(.,'Piscine')]//following-sibling::div/text()").get()
        if swimming_pool!='0':
            item_loader.add_value("swimming_pool",True)
        
        garage=response.xpath(
            "//div[@class='criteres-list']/ul/li/div[contains(.,'Garage')]//following-sibling::div/text()").get()
        parking=response.xpath(
            "//div[@class='criteres-list']/ul/li/div[contains(.,'Parking')]//following-sibling::div/text()").get()
        if garage!='0' or parking!='0':
            item_loader.add_value("parking",True)
        
        images=[x for x in response.xpath("//div[@id='flex_slider_bien']/ul/li/a/@href").getall()]    
        for image in images:
            item_loader.add_value("images", image.strip())
        item_loader.add_value("external_images_count", str(len(images)))
        
        land_name = response.xpath("//p[@class='name']/text()").get()
        item_loader.add_value("landlord_name", land_name)
        
        phone="".join(response.xpath("//div[@class='conteneur']//div[@class='contact_nego_boutons_return']/a/text()").getall())
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        item_loader.add_value("landlord_email", "romain.durand@eraimmo.fr")
        
        yield item_loader.load_item()